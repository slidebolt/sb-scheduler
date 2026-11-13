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

func TestTimerPublishesOnAndOffEdges(t *testing.T) {
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

	commands := make(chan string, 2)
	sub, err := msg.Subscribe("plugin-test.device.recording.command.>", func(m *messenger.Message) { commands <- m.Subject })
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
	_, err = client.CreateTimer(scheduler.CreateTimerRequest{ID: "integration", Name: "Integration", Timer: domain.Timer{Enabled: true, Target: "plugin-test.device.recording", On: domain.TimerTrigger{Kind: "at", At: &on}, Off: domain.TimerTrigger{Kind: "at", At: &off}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"plugin-test.device.recording.command.switch_turn_on", "plugin-test.device.recording.command.switch_turn_off"} {
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
