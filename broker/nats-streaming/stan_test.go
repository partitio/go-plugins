package stan

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec/json"
	"github.com/micro/go-micro/codec/proto"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

var addrTestCases = []struct {
	name        string
	description string
	addrs       map[string]string // expected address : set address
}{
	{
		"brokerOpts",
		"set broker addresses through a broker.Option in constructor",
		map[string]string{
			"nats://192.168.10.1:5222": "192.168.10.1:5222",
			"nats://10.20.10.0:4222":   "10.20.10.0:4222"},
	},
	{
		"brokerInit",
		"set broker addresses through a broker.Option in broker.Init()",
		map[string]string{
			"nats://192.168.10.1:5222": "192.168.10.1:5222",
			"nats://10.20.10.0:4222":   "10.20.10.0:4222"},
	},
	{
		"natsOpts",
		"set broker addresses through the nats.Option in constructor",
		map[string]string{
			"nats://192.168.10.1:5222": "192.168.10.1:5222",
			"nats://10.20.10.0:4222":   "10.20.10.0:4222"},
	},
	{
		"default",
		"check if default Address is set correctly",
		map[string]string{
			"nats://127.0.0.1:4222": "",
		},
	},
}

// TestInitAddrs tests issue #100. Ensures that if the addrs is set by an option in init it will be used.
func TestInitAddrs(t *testing.T) {

	for _, tc := range addrTestCases {
		t.Run(fmt.Sprintf("%s: %s", tc.name, tc.description), func(t *testing.T) {

			var br broker.Broker
			var addrs []string

			for _, addr := range tc.addrs {
				addrs = append(addrs, addr)
			}

			switch tc.name {
			case "brokerOpts":
				// we know that there are just two addrs in the dict
				br = NewBroker(broker.Addrs(addrs[0], addrs[1]))
				br.Init()
			case "brokerInit":
				br = NewBroker()
				// we know that there are just two addrs in the dict
				br.Init(broker.Addrs(addrs[0], addrs[1]))
			case "natsOpts":
				nopts := nats.GetDefaultOptions()
				nopts.Servers = addrs
				br = NewBroker(NatsOptions(nopts))
				br.Init()
			case "default":
				br = NewBroker()
				br.Init()
			}

			natsBroker, ok := br.(*nbroker)
			if !ok {
				t.Fatal("Expected broker to be of types *nbroker")
			}
			// check if the same amount of addrs we set has actually been set, default
			// have only 1 address nats://127.0.0.1:4222 (current nats code) or
			// nats://localhost:4222 (older code version)
			if len(natsBroker.addrs) != len(tc.addrs) && tc.name != "default" {
				t.Errorf("Expected Addr count = %d, Actual Addr count = %d",
					len(natsBroker.addrs), len(tc.addrs))
			}

			for _, addr := range natsBroker.addrs {
				_, ok := tc.addrs[addr]
				if !ok {
					t.Errorf("Expected '%s' has not been set", addr)
				}
			}
		})

	}
}

func TestNewBroker(t *testing.T) {
	// default client use uuid ... so we have to cheat ..
	defaultClientId = "micro-client"
	type args struct {
		opts []broker.Option
	}
	tests := []struct {
		name string
		args args
		want broker.Broker
	}{
		{
			name: "default",
			args: args{
				opts: nil,
			},
			want: &nbroker{
				RWMutex:   sync.RWMutex{},
				clusterId: DefaultClusterId,
				clientId:  defaultClientId,
				addrs:     []string{nats.DefaultURL},
				conn:      nil,
				nconn:     nil,
				opts: broker.Options{
					Codec:   json.Marshaler{},
					Context: context.Background(),
				},
				nopts: nats.GetDefaultOptions(),
				drain: false,
			},
		},
		{
			name: "default",
			args: args{
				opts: []broker.Option{
					broker.Addrs("nats://nats:4222"),
					broker.Codec(proto.Marshaler{}),
					ClusterId("micro-cluster"),
					DrainConnection(),
				},
			},
			want: &nbroker{
				RWMutex:   sync.RWMutex{},
				clusterId: "micro-cluster",
				clientId:  defaultClientId,
				addrs:     []string{"nats://nats:4222"},
				conn:      nil,
				nconn:     nil,
				opts: broker.Options{
					Addrs: []string{"nats://nats:4222"},
					Codec: proto.Marshaler{},
					Context: ctxWithKVPairs(
						clusterIdKey{}, "micro-cluster",
						drainConnectionKey{}, true,
					),
				},
				nopts: nats.GetDefaultOptions(),
				drain: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBroker(tt.args.opts...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBroker() = %v, want %v", got, tt.want)
			}
			// got := NewBroker(tt.args.opts...)
			// assert.Equal(t, tt.want, got)
		})
	}
}

func TestBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ss, err := stand.Run(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Shutdown()

	// Force error with wrong clusterId
	b := NewBroker(ClusterId("noop"))
	if err := b.Connect(); err != stan.ErrConnectReqTimeout {
		t.Fatal("connection should fails")
	}

	b = NewBroker()
	if err := b.Connect(); err != nil {
		t.Fatal(err)
	}

	topic := "test"
	message := "whatever"

	received := make(chan struct{})
	s, err := b.Subscribe(topic, func(event broker.Event) error {
		got := string(event.Message().Body)
		if event.Topic() != topic {
			t.Errorf("expected topic to be: %s, got: %s", topic, event.Topic())
		}
		if len(event.Message().Header) != 0 {
			t.Errorf("expected no headers, got: %v", event.Message().Header)
		}
		if got != message {
			t.Errorf("got: %s, expected to have: %s", got, message)
		}
		close(received)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(topic, &broker.Message{
		Header: nil,
		Body:   []byte(message),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-received:
	}

	if err := s.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := b.Disconnect(); err != nil {
		t.Fatal(err)
	}
}

func ctxWithKVPairs(kv ...interface{}) context.Context {
	ctx := context.Background()
	for i := 0; i < len(kv); i += 2 {
		ctx = context.WithValue(ctx, kv[i], kv[i+1])
	}
	return ctx
}
