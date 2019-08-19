package stan

import (
	"github.com/micro/go-micro/broker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

type clusterIdKey struct{}
type clientIdKey struct{}
type optionsKey struct{}
type natsOptionKey struct{}
type subscriptionOptionsKey struct{}
type drainSubscriptionKey struct{}
type drainConnectionKey struct{}
type ackOnSuccessKey struct{}
type durableName struct{}

func ClusterId(id string) broker.Option {
	return setBrokerOption(clusterIdKey{}, id)
}

func ClientId(id string) broker.Option {
	return setBrokerOption(clientIdKey{}, id)
}

// NatsOptions accepts nats.Options
func NatsOptions(opts nats.Options) broker.Option {
	return setBrokerOption(natsOptionKey{}, opts)
}

// Options accepts stan.Options
func Options(opts ...stan.Option) broker.Option {
	return setBrokerOption(optionsKey{}, opts)
}

// SubscriptionOption accepts stan.SubscriptionOption
func SubscriptionOptions(opts ...stan.SubscriptionOption) broker.SubscribeOption {
	return setSubscribeOption(subscriptionOptionsKey{}, opts)
}

// DrainConnection will drain subscription on close
func DrainConnection() broker.Option {
	return setBrokerOption(drainConnectionKey{}, true)
}

// DrainSubscription will drain pending messages when unsubscribe
func DrainSubscription() broker.SubscribeOption {
	return setSubscribeOption(drainSubscriptionKey{}, true)
}

// AckOnSuccess will automatically acknowledge messages when no error is returned
func AckOnSuccess() broker.SubscribeOption {
	return setSubscribeOption(ackOnSuccessKey{}, true)
}

// DurableName sets the DurableName for the subscriber
func DurableName(name string) broker.SubscribeOption {
	return setSubscribeOption(durableName{}, name)
}

func parseSubscriptionOption(o broker.SubscribeOptions) (opts []stan.SubscriptionOption) {
	if o.Context == nil {
		return opts
	}
	ctx := o.Context
	if os, ok := ctx.Value(subscriptionOptionsKey{}).([]stan.SubscriptionOption); ok {
		opts = append(opts, os...)
	}
	if n, ok := ctx.Value(durableName{}).(string); ok {
		opts = append(opts, stan.DurableName(n))
	}
	if _, ok := ctx.Value(ackOnSuccessKey{}).(bool); ok {
		opts = append(opts, stan.SetManualAckMode())
	}
	return opts
}
