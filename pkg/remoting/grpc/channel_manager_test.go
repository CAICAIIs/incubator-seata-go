/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"seata.apache.org/seata-go/v2/pkg/discovery"
	"seata.apache.org/seata-go/v2/pkg/remoting/config"
	"seata.apache.org/seata-go/v2/pkg/remoting/loadbalance"
)

func TestChannelManagerRefreshesServerListFromRegistrySubscription(t *testing.T) {
	registry := &fakeSubscriberRegistry{
		fakeLookupRegistry: fakeLookupRegistry{
			instances: []*discovery.ServiceInstance{{Addr: "127.0.0.1", Port: 8091}},
		},
	}
	started := make(chan string, 2)
	manager := newTestChannelManager(func(instance *discovery.ServiceInstance) {
		started <- serverAddress(instance)
	})

	manager.initWithRegistry(registry)
	assert.Equal(t, "default_tx_group", registry.subscribeKey)
	assert.Equal(t, "127.0.0.1:8091", nextStartedChannel(t, started))

	registry.publish([]*discovery.ServiceInstance{{Addr: "127.0.0.2", Port: 8092}})
	assert.Equal(t, "127.0.0.2:8092", nextStartedChannel(t, started))
	assert.False(t, manager.isServerAddressAvailable("127.0.0.1:8091"))
	assert.True(t, manager.isServerAddressAvailable("127.0.0.2:8092"))

	registry.publish([]*discovery.ServiceInstance{{Addr: "127.0.0.1", Port: 8091}})
	select {
	case address := <-started:
		t.Fatalf("started a duplicate channel for retained address %s", address)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestChannelManagerFallsBackToLookupWhenRegistryDoesNotSubscribe(t *testing.T) {
	registry := &fakeLookupRegistry{
		instances: []*discovery.ServiceInstance{{Addr: "127.0.0.1", Port: 8091}},
	}
	started := make(chan string, 1)
	manager := newTestChannelManager(func(instance *discovery.ServiceInstance) {
		started <- serverAddress(instance)
	})

	manager.initWithRegistry(registry)
	assert.Equal(t, "default_tx_group", registry.lookupKey)
	assert.Equal(t, "127.0.0.1:8091", nextStartedChannel(t, started))
	assert.True(t, manager.isServerAddressAvailable("127.0.0.1:8091"))
}

func TestChannelManagerSelectChannelSkipsRemovedServerAddress(t *testing.T) {
	previousLoadBalance := loadbalance.GetLoadBalanceConfig()
	loadbalance.InitLoadBalanceConfig(loadbalance.Config{Type: "RoundRobinLoadBalance"})
	t.Cleanup(func() {
		loadbalance.InitLoadBalanceConfig(previousLoadBalance)
	})

	manager := newTestChannelManager(nil)
	manager.refreshServerList([]*discovery.ServiceInstance{
		{Addr: "127.0.0.1", Port: 8091},
		{Addr: "127.0.0.2", Port: 8092},
	})

	removed := newSelectableTestChannel("127.0.0.1:8091")
	kept := newSelectableTestChannel("127.0.0.2:8092")
	assert.True(t, manager.registerChannel(removed))
	assert.True(t, manager.registerChannel(kept))
	manager.refreshServerList([]*discovery.ServiceInstance{{Addr: "127.0.0.2", Port: 8092}})

	assert.Equal(t, kept, manager.selectChannel(&struct{ TransactionName string }{TransactionName: "tx"}))
}

func newTestChannelManager(startChannel func(*discovery.ServiceInstance)) *ChannelManager {
	return &ChannelManager{
		config:       &config.Config{},
		seataConfig:  &config.SeataConfig{TxServiceGroup: "default_tx_group"},
		startChannel: startChannel,
	}
}

func newSelectableTestChannel(addr string) *Channel {
	return &Channel{addr: addr, closeCh: make(chan struct{})}
}

func nextStartedChannel(t *testing.T, started <-chan string) string {
	t.Helper()

	select {
	case address := <-started:
		return address
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for gRPC channel start")
	}
	return ""
}

type fakeLookupRegistry struct {
	lookupKey string
	instances []*discovery.ServiceInstance
}

func (r *fakeLookupRegistry) Lookup(key string) ([]*discovery.ServiceInstance, error) {
	r.lookupKey = key
	return cloneTestServiceInstances(r.instances), nil
}

func (r *fakeLookupRegistry) Close() {}

type fakeSubscriberRegistry struct {
	fakeLookupRegistry
	subscribeKey string
	listener     discovery.RegistryChangeListener
}

func (r *fakeSubscriberRegistry) Subscribe(key string, listener discovery.RegistryChangeListener) (discovery.RegistrySubscription, error) {
	r.subscribeKey = key
	r.listener = listener
	listener(discovery.RegistryChangeEvent{Key: key, Instances: cloneTestServiceInstances(r.instances)})
	return fakeRegistrySubscription{}, nil
}

func (r *fakeSubscriberRegistry) publish(instances []*discovery.ServiceInstance) {
	r.listener(discovery.RegistryChangeEvent{Key: r.subscribeKey, Instances: cloneTestServiceInstances(instances)})
}

type fakeRegistrySubscription struct{}

func (fakeRegistrySubscription) Unsubscribe() {}

func cloneTestServiceInstances(instances []*discovery.ServiceInstance) []*discovery.ServiceInstance {
	clones := make([]*discovery.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		clone := *instance
		clones = append(clones, &clone)
	}
	return clones
}
