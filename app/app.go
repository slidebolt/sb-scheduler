package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	contract "github.com/slidebolt/sb-contract"
	domain "github.com/slidebolt/sb-domain"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	scheduler "github.com/slidebolt/sb-scheduler-sdk"
	storage "github.com/slidebolt/sb-storage-sdk"
)

const ServiceID = "scheduler"
const entityPlugin = "sb-scheduler"

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

type timerEntry struct {
	Entity domain.Entity
	Timer  domain.Timer
}

type App struct {
	msg    messenger.Messenger
	store  storage.Storage
	cmds   *messenger.Commands
	subs   []messenger.Subscription
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	timers map[string]*timerEntry
	now    func() time.Time
}

func New() *App { return &App{timers: map[string]*timerEntry{}, now: time.Now} }

func (a *App) Hello() contract.HelloResponse {
	return contract.HelloResponse{ID: ServiceID, Kind: contract.KindService, ContractVersion: contract.ContractVersion, DependsOn: []string{"messenger", "storage"}}
}

func (a *App) OnStart(deps map[string]json.RawMessage) (json.RawMessage, error) {
	msg, err := messenger.Connect(deps)
	if err != nil {
		return nil, fmt.Errorf("connect messenger: %w", err)
	}
	store, err := storage.Connect(deps)
	if err != nil {
		msg.Close()
		return nil, fmt.Errorf("connect storage: %w", err)
	}
	a.msg, a.store = msg, store
	if err := a.load(); err != nil {
		return nil, err
	}
	for subject, handler := range map[string]func(*messenger.Message){scheduler.SubjectCreate: a.handleCreate, scheduler.SubjectUpdate: a.handleUpdate, scheduler.SubjectDelete: a.handleDelete} {
		sub, err := msg.Subscribe(subject, handler)
		if err != nil {
			return nil, err
		}
		a.subs = append(a.subs, sub)
	}
	a.cmds = messenger.NewCommands(msg, domain.LookupCommand)
	sub, err := a.cmds.Receive(entityPlugin+".>", a.handleCommand)
	if err != nil {
		return nil, err
	}
	a.subs = append(a.subs, sub)
	a.ctx, a.cancel = context.WithCancel(context.Background())
	go a.run(a.ctx)
	log.Println("sb-scheduler: started")
	return nil, nil
}

func (a *App) OnShutdown() error {
	if a.cancel != nil {
		a.cancel()
	}
	for _, sub := range a.subs {
		_ = sub.Unsubscribe()
	}
	if a.store != nil {
		a.store.Close()
	}
	if a.msg != nil {
		a.msg.Close()
	}
	return nil
}

func timerKey(id string) domain.EntityKey {
	return domain.EntityKey{Plugin: entityPlugin, DeviceID: "timer-" + id, ID: "timer"}
}

func (a *App) load() error {
	entries, err := a.store.Search(entityPlugin + ".timer-*.timer")
	if err != nil {
		return fmt.Errorf("load timers: %w", err)
	}
	now := a.now()
	for _, item := range entries {
		var entity domain.Entity
		if json.Unmarshal(item.Data, &entity) != nil {
			continue
		}
		timer, ok := entity.State.(domain.Timer)
		if !ok {
			continue
		}
		prepareNext(&timer, now)
		entity.State = timer
		a.timers[entity.Key()] = &timerEntry{Entity: entity, Timer: timer}
		_ = a.store.Save(entity)
	}
	return nil
}

func (a *App) run(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			a.tick(now)
		}
	}
}

func (a *App) tick(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, entry := range a.timers {
		if !entry.Timer.Enabled {
			continue
		}
		if entry.Timer.NextOnAt != nil && !entry.Timer.NextOnAt.After(now) {
			a.fire(entry, "on", now)
		}
		if entry.Timer.NextOffAt != nil && !entry.Timer.NextOffAt.After(now) {
			a.fire(entry, "off", now)
		}
	}
}

func (a *App) fire(entry *timerEntry, edge string, now time.Time) {
	target, err := parseEntityKey(entry.Timer.Target)
	if err == nil {
		if edge == "on" {
			err = a.cmds.Send(target, domain.SwitchTurnOn{Input: entry.Timer.Input})
		} else {
			err = a.cmds.Send(target, domain.SwitchTurnOff{})
		}
	}
	entry.Timer.LastEdge, entry.Timer.LastRunAt = edge, timePtr(now)
	if err != nil {
		entry.Timer.LastError = err.Error()
	} else {
		entry.Timer.LastError = ""
	}
	if edge == "on" {
		entry.Timer.NextOnAt = nextTime(entry.Timer.On, now)
	} else {
		entry.Timer.NextOffAt = nextTime(entry.Timer.Off, now)
	}
	if entry.Timer.On.Kind == "at" && entry.Timer.Off.Kind == "at" && entry.Timer.NextOnAt == nil && entry.Timer.NextOffAt == nil {
		entry.Timer.Enabled = false
	}
	entry.Entity.State = entry.Timer
	if err := a.store.Save(entry.Entity); err != nil {
		log.Printf("sb-scheduler: save %s: %v", entry.Entity.Key(), err)
	}
}

func prepareNext(timer *domain.Timer, now time.Time) {
	if !timer.Enabled {
		timer.NextOnAt, timer.NextOffAt = nil, nil
		return
	}
	timer.NextOnAt = nextTime(timer.On, now)
	timer.NextOffAt = nextTime(timer.Off, now)
}

func nextTime(trigger domain.TimerTrigger, after time.Time) *time.Time {
	switch trigger.Kind {
	case "at":
		if trigger.At != nil && trigger.At.After(after) {
			t := trigger.At.UTC()
			return &t
		}
	case "cron":
		loc := time.Local
		if trigger.TimeZone != "" {
			if parsed, err := time.LoadLocation(trigger.TimeZone); err == nil {
				loc = parsed
			} else {
				return nil
			}
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		schedule, err := parser.Parse(trigger.Cron)
		if err != nil {
			return nil
		}
		next := schedule.Next(after.In(loc))
		return &next
	}
	return nil
}

func (a *App) save(id, name string, timer domain.Timer) (domain.EntityKey, error) {
	if !idPattern.MatchString(id) {
		return domain.EntityKey{}, fmt.Errorf("invalid timer id %q", id)
	}
	if _, err := parseEntityKey(timer.Target); err != nil {
		return domain.EntityKey{}, err
	}
	if err := validateTimer(timer); err != nil {
		return domain.EntityKey{}, err
	}
	if name == "" {
		name = id
	}
	prepareNext(&timer, a.now())
	key := timerKey(id)
	entity := domain.Entity{ID: key.ID, Plugin: key.Plugin, DeviceID: key.DeviceID, Type: "timer", Name: name, Commands: []string{"timer_enable", "timer_disable"}, State: timer}
	if err := a.store.Save(domain.Device{ID: key.DeviceID, Plugin: key.Plugin, Name: name}); err != nil {
		return key, err
	}
	if err := a.store.Save(entity); err != nil {
		return key, err
	}
	a.mu.Lock()
	a.timers[key.Key()] = &timerEntry{Entity: entity, Timer: timer}
	a.mu.Unlock()
	return key, nil
}

func validateTimer(timer domain.Timer) error {
	for name, trigger := range map[string]domain.TimerTrigger{"on": timer.On, "off": timer.Off} {
		switch trigger.Kind {
		case "at":
			if trigger.At == nil {
				return fmt.Errorf("%s trigger requires at", name)
			}
		case "cron":
			if trigger.TimeZone != "" {
				if _, err := time.LoadLocation(trigger.TimeZone); err != nil {
					return fmt.Errorf("%s trigger timezone: %w", name, err)
				}
			}
			parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
			if _, err := parser.Parse(trigger.Cron); err != nil {
				return fmt.Errorf("%s trigger cron: %w", name, err)
			}
		default:
			return fmt.Errorf("%s trigger kind must be at or cron", name)
		}
	}
	if timer.On.Kind == "at" && timer.Off.Kind == "at" && !timer.On.At.Before(*timer.Off.At) {
		return fmt.Errorf("on time must be before off time")
	}
	return nil
}

func (a *App) handleCreate(m *messenger.Message) {
	var req scheduler.CreateTimerRequest
	var reply scheduler.TimerReply
	if err := json.Unmarshal(m.Data, &req); err != nil {
		reply.Error = "invalid request"
	} else {
		id := req.ID
		if id == "" {
			id = fmt.Sprintf("%d", a.now().UnixNano())
		}
		var err error
		reply.Key, err = a.save(id, req.Name, req.Timer)
		if err != nil {
			reply.Error = err.Error()
		}
	}
	respond(m, reply)
}

func (a *App) handleUpdate(m *messenger.Message) {
	var req scheduler.UpdateTimerRequest
	var reply scheduler.TimerReply
	if err := json.Unmarshal(m.Data, &req); err != nil {
		reply.Error = "invalid request"
	} else {
		id := strings.TrimPrefix(req.Key.DeviceID, "timer-")
		var err error
		reply.Key, err = a.save(id, req.Name, req.Timer)
		if err != nil {
			reply.Error = err.Error()
		}
	}
	respond(m, reply)
}

func (a *App) handleDelete(m *messenger.Message) {
	var req scheduler.DeleteTimerRequest
	var reply scheduler.TimerReply
	if err := json.Unmarshal(m.Data, &req); err != nil {
		reply.Error = "invalid request"
	} else {
		reply.Key = req.Key
		if err := a.store.Delete(req.Key); err != nil {
			reply.Error = err.Error()
		} else {
			a.mu.Lock()
			delete(a.timers, req.Key.Key())
			a.mu.Unlock()
			_ = a.store.Delete(domain.DeviceKey{Plugin: req.Key.Plugin, ID: req.Key.DeviceID})
		}
	}
	respond(m, reply)
}

func (a *App) handleCommand(addr messenger.Address, cmd any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.timers[addr.Key()]
	if entry == nil {
		return
	}
	switch cmd.(type) {
	case domain.TimerEnable:
		entry.Timer.Enabled = true
	case domain.TimerDisable:
		entry.Timer.Enabled = false
	default:
		return
	}
	prepareNext(&entry.Timer, a.now())
	entry.Entity.State = entry.Timer
	_ = a.store.Save(entry.Entity)
}

func parseEntityKey(value string) (domain.EntityKey, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return domain.EntityKey{}, fmt.Errorf("target must be plugin.device.entity")
	}
	return domain.EntityKey{Plugin: parts[0], DeviceID: parts[1], ID: parts[2]}, nil
}

func timePtr(t time.Time) *time.Time      { return &t }
func respond(m *messenger.Message, v any) { data, _ := json.Marshal(v); _ = m.Respond(data) }
