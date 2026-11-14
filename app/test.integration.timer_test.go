package app

import (
	"encoding/json"
	"testing"
	"time"

	domain "github.com/slidebolt/sb-domain"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	scheduler "github.com/slidebolt/sb-scheduler-sdk"
	storageserver "github.com/slidebolt/sb-storage-server"
)

func TestSchedulePublishesGenericActions(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()
	serverMsg, err := messenger.Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer serverMsg.Close()
	handler, err := storageserver.NewHandler()
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Register(serverMsg); err != nil {
		t.Fatal(err)
	}
	if err := serverMsg.Flush(); err != nil {
		t.Fatal(err)
	}

	commands := make(chan string, 5)
	sub, err := msg.Subscribe("plugin-test.>", func(m *messenger.Message) { commands <- m.Subject })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := msg.Flush(); err != nil {
		t.Fatal(err)
	}

	service := New()
	if _, err := service.OnStart(map[string]json.RawMessage{"messenger": payload, "storage": json.RawMessage(`{"client":"mock"}`)}); err != nil {
		t.Fatal(err)
	}
	defer service.OnShutdown()
	now := time.Now()
	on := now.Add(400 * time.Millisecond)
	off := now.Add(800 * time.Millisecond)
	client := scheduler.NewClient(msg)
	_, err = client.CreateSchedule(scheduler.CreateScheduleRequest{
		ID: "integration", Name: "Integration",
		Schedule: domain.Schedule{Enabled: true, Steps: []domain.ScheduleStep{
			{ID: "start", Trigger: domain.Trigger{Kind: "at", At: &on}, Actions: []domain.Action{
				{Target: "plugin-test.device.recording", Command: "record_start", Input: json.RawMessage(`{"source":{"kind":"live","entity":"plugin-test.device.source"}}`)},
			}},
			{ID: "stop", Trigger: domain.Trigger{Kind: "at", At: &off}, Actions: []domain.Action{
				{Target: "plugin-test.device.recording", Command: "record_stop"},
			}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"plugin-test.device.recording.command.record_start", "plugin-test.device.recording.command.record_stop"} {
		select {
		case got := <-commands:
			if got != want {
				t.Fatalf("got %s want %s", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s", want)
		}
	}

	_, err = client.CreateSchedule(scheduler.CreateScheduleRequest{
		ID: "doorbell", Name: "Doorbell response",
		Schedule: domain.Schedule{Enabled: true, Steps: []domain.ScheduleStep{{
			ID: "ring", Trigger: domain.Trigger{Kind: "event", Source: "plugin-frigate.front.event-person", Match: json.RawMessage(`{"state":{"active":true}}`)},
			Actions: []domain.Action{
				{Target: "plugin-test.intercom.endpoint", Command: "sip_call"},
				{Target: "plugin-test.living.player", Command: "media_play"},
				{Target: "plugin-test.pixel.phone", Command: "media_play"},
			},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.Publish("state.changed.plugin-frigate.front.event-person", []byte(`{"state":{"active":true,"lastEventID":"ring-1"}}`)); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"plugin-test.intercom.endpoint.command.sip_call", "plugin-test.living.player.command.media_play", "plugin-test.pixel.phone.command.media_play"} {
		select {
		case got := <-commands:
			if got != want {
				t.Fatalf("got %s want %s", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}
