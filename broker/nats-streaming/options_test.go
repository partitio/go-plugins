package stan

import (
	"reflect"
	"testing"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
)

func Test_parseSubscriptionOption(t *testing.T) {
	type args struct {
		o []broker.SubscribeOption
	}
	tests := []struct {
		name     string
		args     args
		wantOpts *stan.SubscriptionOptions
	}{
		{
			name:     "empty",
			wantOpts: &stan.SubscriptionOptions{},
		},
		{
			name: "durable name",
			args: args{o: []broker.SubscribeOption{
				DurableName("i-will-remember"),
			}},
			wantOpts: &stan.SubscriptionOptions{
				DurableName: "i-will-remember",
			},
		},
		{
			name: "ack on success",
			args: args{o: []broker.SubscribeOption{
				AckOnSuccess(),
			}},
			wantOpts: &stan.SubscriptionOptions{
				ManualAcks: true,
			},
		},
		{
			name: "stan options",
			args: args{o: []broker.SubscribeOption{
				SubscriptionOptions(
					stan.DurableName("other"),
					stan.SetManualAckMode(),
					stan.DeliverAllAvailable(),
					stan.AckWait(2*time.Second),
				),
			}},
			wantOpts: &stan.SubscriptionOptions{
				DurableName: "other",
				AckWait:     2 * time.Second,
				ManualAcks:  true,
				StartAt:     pb.StartPosition_First,
			},
		},
		{
			name: "override stan options",
			args: args{o: []broker.SubscribeOption{
				SubscriptionOptions(
					stan.DurableName("other"),
					stan.DeliverAllAvailable(),
					stan.AckWait(2*time.Second),
				),
				DurableName("noop"),
				AckOnSuccess(),
			}},
			wantOpts: &stan.SubscriptionOptions{
				DurableName: "noop",
				AckWait:     2 * time.Second,
				ManualAcks:  true,
				StartAt:     pb.StartPosition_First,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			so := broker.SubscribeOptions{}
			for _, o := range tt.args.o {
				o(&so)
			}
			opts := parseSubscriptionOption(so)
			gotOpts := &stan.SubscriptionOptions{}
			for _, o := range opts {
				o(gotOpts)
			}
			if !reflect.DeepEqual(gotOpts, tt.wantOpts) {
				t.Errorf("parseSubscriptionOption() = %v, want %v", gotOpts, tt.wantOpts)
			}
		})
	}
}
