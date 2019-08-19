// Package nats provides a NATS broker
package stan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec/json"
	"github.com/micro/go-micro/config/cmd"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

var (
	DefaultClusterId = "test-cluster"
	defaultClientId  = fmt.Sprintf("micro-%s", uuid.New())
)

type nbroker struct {
	sync.RWMutex
	clusterId string
	clientId  string
	addrs     []string
	conn      stan.Conn
	nconn     *nats.Conn
	opts      broker.Options
	nopts     nats.Options
	drain     bool
}

type subscriber struct {
	s     stan.Subscription
	opts  broker.SubscribeOptions
	drain bool
	topic string
}

type publication struct {
	t  string
	m  *broker.Message
	sm *stan.Msg
}

func init() {
	cmd.DefaultBrokers["nats-streaming"] = NewBroker
}

func (n *publication) Topic() string {
	return n.t
}

func (n *publication) Message() *broker.Message {
	return n.m
}

func (n *publication) Ack() error {
	return n.sm.Ack()
}

func (n *subscriber) Options() broker.SubscribeOptions {
	return n.opts
}

func (n *subscriber) Topic() string {
	return n.topic
}

func (n *subscriber) Unsubscribe() error {
	if n.drain {
		return n.s.Unsubscribe()
	}
	return n.s.Close()
}

func (n *nbroker) Address() string {
	if n.nconn != nil && n.nconn.IsConnected() {
		return n.nconn.ConnectedUrl()
	}
	if len(n.addrs) > 0 {
		return n.addrs[0]
	}

	return ""
}

func setAddrs(addrs []string) []string {
	var cAddrs []string
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{nats.DefaultURL}
	}
	return cAddrs
}

func (n *nbroker) Connect() error {
	n.Lock()
	defer n.Unlock()

	status := nats.CLOSED
	if n.nconn != nil {
		status = n.nconn.Status()
	}

	switch status {
	case nats.CONNECTED, nats.RECONNECTING, nats.CONNECTING:
		return nil
	default: // DISCONNECTED or CLOSED or DRAINING
		opts := n.nopts
		opts.Servers = n.addrs
		opts.Secure = n.opts.Secure
		opts.TLSConfig = n.opts.TLSConfig

		// secure might not be set
		if n.opts.TLSConfig != nil {
			opts.Secure = true
		}

		var err error
		n.nconn, err = opts.Connect()
		if err != nil {
			return err
		}
		var sopts []stan.Option
		if os, ok := n.opts.Context.Value(optionsKey{}).([]stan.Option); ok {
			sopts = os
		}
		sopts = append(sopts, stan.NatsConn(n.nconn))
		n.conn, err = stan.Connect(n.clusterId, n.clientId, sopts...)
		return err
	}
}

func (n *nbroker) Disconnect() error {
	n.RLock()
	if n.drain {
		n.nconn.Drain()
	} else {
		n.nconn.Close()
	}
	n.conn.Close()
	n.RUnlock()
	return nil
}

func (n *nbroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&n.opts)
	}
	n.addrs = setAddrs(n.opts.Addrs)
	return nil
}

func (n *nbroker) Options() broker.Options {
	return n.opts
}

func (n *nbroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	b, err := n.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	n.RLock()
	defer n.RUnlock()
	return n.conn.Publish(topic, b)
}

func (n *nbroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	if n.conn == nil {
		return nil, errors.New("not connected")
	}

	opt := broker.SubscribeOptions{
		AutoAck: true,
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&opt)
	}

	var drain bool
	if _, ok := opt.Context.Value(drainSubscriptionKey{}).(bool); ok {
		drain = true
	}

	var ackOnSuccess bool
	if _, ok := opt.Context.Value(ackOnSuccessKey{}).(bool); ok {
		ackOnSuccess = true
	}

	fn := func(msg *stan.Msg) {
		var m broker.Message
		if err := n.opts.Codec.Unmarshal(msg.Data, &m); err != nil {
			return
		}
		if err := handler(&publication{m: &m, sm: msg, t: msg.Subject}); err == nil && ackOnSuccess {
			msg.Ack()
		}
	}

	sopts := parseSubscriptionOption(opt)

	var sub stan.Subscription
	var err error

	n.RLock()
	if len(opt.Queue) > 0 {
		sub, err = n.conn.QueueSubscribe(topic, opt.Queue, fn, sopts...)
	} else {
		sub, err = n.conn.Subscribe(topic, fn, sopts...)
	}
	n.RUnlock()
	if err != nil {
		return nil, err
	}
	return &subscriber{topic: topic, s: sub, opts: opt, drain: drain}, nil
}

func (n *nbroker) String() string {
	return "nats-streaming"
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.Options{
		// Default codec
		Codec:   json.Marshaler{},
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}
	clusterId := DefaultClusterId
	if id, ok := options.Context.Value(clusterIdKey{}).(string); ok {
		clusterId = id
	}

	clientId := defaultClientId
	if id, ok := options.Context.Value(clientIdKey{}).(string); ok {
		clientId = id
	}

	natsOpts := nats.GetDefaultOptions()
	if n, ok := options.Context.Value(natsOptionKey{}).(nats.Options); ok {
		natsOpts = n
	}

	var drain bool
	if _, ok := options.Context.Value(drainConnectionKey{}).(bool); ok {
		drain = true
	}

	// broker.Options have higher priority than nats.Options
	// only if Addrs, Secure or TLSConfig were not set through a broker.Option
	// we read them from nats.Option
	if len(options.Addrs) == 0 {
		options.Addrs = natsOpts.Servers
	}

	if !options.Secure {
		options.Secure = natsOpts.Secure
	}

	if options.TLSConfig == nil {
		options.TLSConfig = natsOpts.TLSConfig
	}

	nb := &nbroker{
		clusterId: clusterId,
		clientId:  clientId,
		opts:      options,
		nopts:     natsOpts,
		addrs:     setAddrs(options.Addrs),
		drain:     drain,
	}

	return nb
}
