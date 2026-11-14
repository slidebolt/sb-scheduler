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

type scheduleEntry struct {
	Entity   domain.Entity
	Schedule domain.Schedule
}
type App struct {
	msg       messenger.Messenger
	store     storage.Storage
	cmds      *messenger.Commands
	subs      []messenger.Subscription
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	schedules map[string]*scheduleEntry
	now       func() time.Time
}

func New() *App { return &App{schedules: map[string]*scheduleEntry{}, now: time.Now} }
func (a *App) Hello() contract.HelloResponse {
	return contract.HelloResponse{ID: ServiceID, Kind: contract.KindService, ContractVersion: contract.ContractVersion, DependsOn: []string{"messenger", "storage"}, Setup: contract.RequiredSetup("Core")}
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
	a.msg, a.store, a.cmds = msg, store, messenger.NewCommands(msg, domain.LookupCommand)
	if err := a.load(); err != nil {
		return nil, err
	}
	for subject, handler := range map[string]func(*messenger.Message){scheduler.SubjectCreate: a.handleCreate, scheduler.SubjectUpdate: a.handleUpdate, scheduler.SubjectDelete: a.handleDelete, "state.changed.>": a.handleEvent} {
		sub, err := msg.Subscribe(subject, handler)
		if err != nil {
			return nil, err
		}
		a.subs = append(a.subs, sub)
	}
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
	for _, s := range a.subs {
		_ = s.Unsubscribe()
	}
	if a.store != nil {
		a.store.Close()
	}
	if a.msg != nil {
		a.msg.Close()
	}
	return nil
}

func scheduleKey(id string) domain.EntityKey {
	return domain.EntityKey{Plugin: entityPlugin, DeviceID: "schedule-" + id, ID: "schedule"}
}
func (a *App) load() error {
	entries, err := a.store.Search(entityPlugin + ".schedule-*.schedule")
	if err != nil {
		return fmt.Errorf("load schedules: %w", err)
	}
	for _, item := range entries {
		var entity domain.Entity
		if json.Unmarshal(item.Data, &entity) != nil {
			continue
		}
		schedule, ok := entity.State.(domain.Schedule)
		if !ok {
			continue
		}
		prepare(&schedule, a.now())
		entity.State = schedule
		a.schedules[entity.Key()] = &scheduleEntry{entity, schedule}
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
	for _, entry := range a.schedules {
		if !entry.Schedule.Enabled {
			continue
		}
		changed := false
		for i := range entry.Schedule.Steps {
			step := &entry.Schedule.Steps[i]
			if step.NextRunAt != nil && !step.NextRunAt.After(now) {
				a.fire(step, now)
				step.NextRunAt = nextTime(step.Trigger, now)
				changed = true
			}
		}
		if changed {
			a.finish(entry)
			a.persist(entry)
		}
	}
}
func (a *App) fire(step *domain.ScheduleStep, now time.Time) {
	errs := []string{}
	for _, action := range step.Actions {
		target, err := parseEntityKey(action.Target)
		if err == nil {
			err = a.cmds.SendRaw(target, action.Command, action.Input)
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	step.LastRunAt = &now
	step.LastError = strings.Join(errs, "; ")
}
func (a *App) finish(entry *scheduleEntry) {
	for _, step := range entry.Schedule.Steps {
		if step.Trigger.Kind == "event" || step.NextRunAt != nil {
			return
		}
	}
	entry.Schedule.Enabled = false
}
func (a *App) persist(entry *scheduleEntry) {
	entry.Entity.State = entry.Schedule
	if err := a.store.Save(entry.Entity); err != nil {
		log.Printf("sb-scheduler: save %s: %v", entry.Entity.Key(), err)
	}
}
func prepare(schedule *domain.Schedule, now time.Time) {
	for i := range schedule.Steps {
		schedule.Steps[i].NextRunAt = nextTime(schedule.Steps[i].Trigger, now)
	}
}
func nextTime(trigger domain.Trigger, after time.Time) *time.Time {
	switch trigger.Kind {
	case "at":
		if trigger.At != nil && trigger.At.After(after) {
			t := trigger.At.UTC()
			return &t
		}
	case "cron":
		loc := time.Local
		if trigger.TimeZone != "" {
			parsed, err := time.LoadLocation(trigger.TimeZone)
			if err != nil {
				return nil
			}
			loc = parsed
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		parsed, err := parser.Parse(trigger.Cron)
		if err != nil {
			return nil
		}
		next := parsed.Next(after.In(loc))
		return &next
	}
	return nil
}

func (a *App) handleEvent(m *messenger.Message) {
	source := strings.TrimPrefix(m.Subject, "state.changed.")
	var envelope map[string]any
	if json.Unmarshal(m.Data, &envelope) != nil {
		return
	}
	state, _ := envelope["state"].(map[string]any)
	eventName, _ := state["lastEventID"].(string)
	if eventName == "" {
		eventName, _ = state["event"].(string)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	for _, entry := range a.schedules {
		if !entry.Schedule.Enabled {
			continue
		}
		changed := false
		for i := range entry.Schedule.Steps {
			step := &entry.Schedule.Steps[i]
			if step.Trigger.Kind != "event" || step.Trigger.Source != source || step.Trigger.Event != "" && step.Trigger.Event != eventName || !matches(step.Trigger.Match, envelope) {
				continue
			}
			a.fire(step, now)
			changed = true
		}
		if changed {
			a.persist(entry)
		}
	}
}
func matches(raw json.RawMessage, actual map[string]any) bool {
	if len(raw) == 0 {
		return true
	}
	var want map[string]any
	if json.Unmarshal(raw, &want) != nil {
		return false
	}
	return subset(want, actual)
}
func subset(want, actual map[string]any) bool {
	for key, w := range want {
		a, ok := actual[key]
		if !ok {
			return false
		}
		wm, wok := w.(map[string]any)
		am, aok := a.(map[string]any)
		if wok {
			if !aok || !subset(wm, am) {
				return false
			}
		} else {
			wb, _ := json.Marshal(w)
			ab, _ := json.Marshal(a)
			if string(wb) != string(ab) {
				return false
			}
		}
	}
	return true
}

func (a *App) save(id, name string, schedule domain.Schedule) (domain.EntityKey, error) {
	if !idPattern.MatchString(id) {
		return domain.EntityKey{}, fmt.Errorf("invalid schedule id %q", id)
	}
	if err := validateSchedule(schedule); err != nil {
		return domain.EntityKey{}, err
	}
	if name == "" {
		name = id
	}
	prepare(&schedule, a.now())
	key := scheduleKey(id)
	entity := domain.Entity{ID: key.ID, Plugin: key.Plugin, DeviceID: key.DeviceID, Type: "schedule", Name: name, Commands: []string{"schedule_enable", "schedule_disable"}, State: schedule}
	if err := a.store.Save(domain.Device{ID: key.DeviceID, Plugin: key.Plugin, Name: name}); err != nil {
		return key, err
	}
	if err := a.store.Save(entity); err != nil {
		return key, err
	}
	a.mu.Lock()
	a.schedules[key.Key()] = &scheduleEntry{entity, schedule}
	a.mu.Unlock()
	return key, nil
}
func validateSchedule(schedule domain.Schedule) error {
	if len(schedule.Steps) == 0 {
		return fmt.Errorf("schedule requires at least one step")
	}
	seen := map[string]bool{}
	for i, step := range schedule.Steps {
		if step.ID == "" {
			return fmt.Errorf("step %d requires id", i)
		}
		if seen[step.ID] {
			return fmt.Errorf("duplicate step id %q", step.ID)
		}
		seen[step.ID] = true
		if len(step.Actions) == 0 {
			return fmt.Errorf("step %s requires actions", step.ID)
		}
		for _, action := range step.Actions {
			if _, err := parseEntityKey(action.Target); err != nil {
				return err
			}
			if strings.TrimSpace(action.Command) == "" {
				return fmt.Errorf("step %s action command is required", step.ID)
			}
		}
		switch step.Trigger.Kind {
		case "at":
			if step.Trigger.At == nil {
				return fmt.Errorf("step %s at trigger requires at", step.ID)
			}
		case "cron":
			if nextTime(step.Trigger, time.Now()) == nil {
				return fmt.Errorf("step %s has invalid cron trigger", step.ID)
			}
		case "event":
			if step.Trigger.Source == "" {
				return fmt.Errorf("step %s event trigger requires source", step.ID)
			}
		default:
			return fmt.Errorf("step %s trigger kind must be at, cron, or event", step.ID)
		}
	}
	return nil
}

func (a *App) handleCreate(m *messenger.Message) {
	var req scheduler.CreateScheduleRequest
	var reply scheduler.ScheduleReply
	if json.Unmarshal(m.Data, &req) != nil {
		reply.Error = "invalid request"
	} else {
		id := req.ID
		if id == "" {
			id = fmt.Sprintf("%d", a.now().UnixNano())
		}
		var err error
		reply.Key, err = a.save(id, req.Name, req.Schedule)
		if err != nil {
			reply.Error = err.Error()
		}
	}
	respond(m, reply)
}
func (a *App) handleUpdate(m *messenger.Message) {
	var req scheduler.UpdateScheduleRequest
	var reply scheduler.ScheduleReply
	if json.Unmarshal(m.Data, &req) != nil {
		reply.Error = "invalid request"
	} else {
		id := strings.TrimPrefix(req.Key.DeviceID, "schedule-")
		var err error
		reply.Key, err = a.save(id, req.Name, req.Schedule)
		if err != nil {
			reply.Error = err.Error()
		}
	}
	respond(m, reply)
}
func (a *App) handleDelete(m *messenger.Message) {
	var req scheduler.DeleteScheduleRequest
	var reply scheduler.ScheduleReply
	if json.Unmarshal(m.Data, &req) != nil {
		reply.Error = "invalid request"
	} else {
		reply.Key = req.Key
		if err := a.store.Delete(req.Key); err != nil {
			reply.Error = err.Error()
		} else {
			a.mu.Lock()
			delete(a.schedules, req.Key.Key())
			a.mu.Unlock()
			_ = a.store.Delete(domain.DeviceKey{Plugin: req.Key.Plugin, ID: req.Key.DeviceID})
		}
	}
	respond(m, reply)
}
func (a *App) handleCommand(addr messenger.Address, cmd any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.schedules[addr.Key()]
	if entry == nil {
		return
	}
	switch cmd.(type) {
	case domain.ScheduleEnable:
		entry.Schedule.Enabled = true
	case domain.ScheduleDisable:
		entry.Schedule.Enabled = false
	default:
		return
	}
	prepare(&entry.Schedule, a.now())
	a.persist(entry)
}
func parseEntityKey(value string) (domain.EntityKey, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return domain.EntityKey{}, fmt.Errorf("target must be plugin.device.entity")
	}
	return domain.EntityKey{Plugin: parts[0], DeviceID: parts[1], ID: parts[2]}, nil
}
func respond(m *messenger.Message, v any) { data, _ := json.Marshal(v); _ = m.Respond(data) }
