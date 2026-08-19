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

package getty

import (
	"testing"
	"time"

	getty "github.com/apache/dubbo-getty"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"seata.apache.org/seata-go/v2/pkg/discovery"
	"seata.apache.org/seata-go/v2/pkg/protocol/message"
	"seata.apache.org/seata-go/v2/pkg/remoting/config"
	"seata.apache.org/seata-go/v2/pkg/remoting/loadbalance"
	"seata.apache.org/seata-go/v2/pkg/remoting/mock"
)

func TestSessionManagerRefreshesServerListFromRegistrySubscription(t *testing.T) {
	registry := &fakeSubscriberRegistry{
		fakeLookupRegistry: fakeLookupRegistry{
			instances: []*discovery.ServiceInstance{{Addr: "127.0.0.1", Port: 8091}},
		},
	}
	started := make(chan string, 2)
	manager := newTestSessionManager(func(instance *discovery.ServiceInstance) {
		started <- serverAddress(instance)
	})

	manager.initWithRegistry(registry)
	assert.Equal(t, "default_tx_group", registry.subscribeKey)
	assert.Equal(t, "127.0.0.1:8091", nextStartedServer(t, started))

	registry.publish([]*discovery.ServiceInstance{{Addr: "127.0.0.2", Port: 8092}})
	assert.Equal(t, "127.0.0.2:8092", nextStartedServer(t, started))
	assert.False(t, manager.isServerAddressAvailable("127.0.0.1:8091"))
	assert.True(t, manager.isServerAddressAvailable("127.0.0.2:8092"))
}

func TestSessionManagerFallsBackToLookupWhenRegistryDoesNotSubscribe(t *testing.T) {
	registry := &fakeLookupRegistry{
		instances: []*discovery.ServiceInstance{{Addr: "127.0.0.1", Port: 8091}},
	}
	started := make(chan string, 1)
	manager := newTestSessionManager(func(instance *discovery.ServiceInstance) {
		started <- serverAddress(instance)
	})

	manager.initWithRegistry(registry)
	assert.Equal(t, "default_tx_group", registry.lookupKey)
	assert.Equal(t, "127.0.0.1:8091", nextStartedServer(t, started))
	assert.True(t, manager.isServerAddressAvailable("127.0.0.1:8091"))
}

func TestSessionManagerSelectSessionSkipsRemovedServerAddress(t *testing.T) {
	previousLoadBalance := loadbalance.GetLoadBalanceConfig()
	loadbalance.InitLoadBalanceConfig(loadbalance.Config{Type: "RoundRobinLoadBalance"})
	t.Cleanup(func() {
		loadbalance.InitLoadBalanceConfig(previousLoadBalance)
	})

	ctrl := gomock.NewController(t)
	manager := newTestSessionManager(nil)
	manager.refreshServerList([]*discovery.ServiceInstance{
		{Addr: "127.0.0.1", Port: 8091},
		{Addr: "127.0.0.2", Port: 8092},
	})

	removed := mock.NewMockTestSession(ctrl)
	removed.EXPECT().IsClosed().Return(false).AnyTimes()
	removed.EXPECT().RemoteAddr().Return("127.0.0.1:8091").AnyTimes()
	kept := mock.NewMockTestSession(ctrl)
	kept.EXPECT().IsClosed().Return(false).AnyTimes()
	kept.EXPECT().RemoteAddr().Return("127.0.0.2:8092").AnyTimes()

	manager.registerSession(removed)
	manager.registerSession(kept)
	manager.refreshServerList([]*discovery.ServiceInstance{{Addr: "127.0.0.2", Port: 8092}})

	selected := manager.selectSession(message.GlobalBeginRequest{TransactionName: "tx"})
	assert.Equal(t, getty.Session(kept), selected)
}

func newTestSessionManager(startClient func(*discovery.ServiceInstance)) *SessionManager {
	return &SessionManager{
		gettyConf:   &config.Config{},
		seataConfig: &config.SeataConfig{TxServiceGroup: "default_tx_group"},
		startClient: startClient,
	}
}

func nextStartedServer(t *testing.T, started <-chan string) string {
	t.Helper()

	select {
	case address := <-started:
		return address
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server client start")
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
