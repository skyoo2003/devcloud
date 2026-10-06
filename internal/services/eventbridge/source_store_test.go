// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPartnerSourceBindingAndBusDeletion(t *testing.T) {
	s := newTestProvider(t).store
	const name = "aws.partner/devcloud/source"
	source := PartnerSource{Name: name, AccountID: defaultAccountID, ARN: "source-arn", State: "PENDING", CreationTime: time.Now().UTC()}
	require.NoError(t, s.CreatePartnerSource(source))
	require.ErrorIs(t, s.CreatePartnerSource(source), ErrAlreadyExists)
	require.ErrorIs(t, s.SetPartnerSourceState(name, defaultAccountID, "ACTIVE"), ErrInvalidSourceState)
	require.ErrorIs(t, s.CreatePartnerEventBus("wrong", name, defaultAccountID), ErrInvalidSourceState)
	_, err := s.GetEventBus("wrong", defaultAccountID)
	require.ErrorIs(t, err, ErrBusNotFound)
	require.NoError(t, s.CreatePartnerEventBus(name, name, defaultAccountID))
	loaded, err := s.GetPartnerSource(name, defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, "ACTIVE", loaded.State)
	require.NoError(t, s.SetPartnerSourceState(name, defaultAccountID, "PENDING"))
	require.NoError(t, s.SetPartnerSourceState(name, defaultAccountID, "ACTIVE"))
	require.NoError(t, s.DeleteEventBus(name, defaultAccountID))
	loaded, err = s.GetPartnerSource(name, defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, "PENDING", loaded.State)
	require.Empty(t, loaded.BusName)
	require.NoError(t, s.DeletePartnerSource(name, defaultAccountID))
	_, err = s.GetPartnerSource(name, defaultAccountID)
	require.ErrorIs(t, err, ErrSourceNotFound)
}

func TestPartnerSourceConcurrentLifecycle(t *testing.T) {
	s := newTestProvider(t).store
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("aws.partner/devcloud/concurrent-%d", i)
		require.NoError(t, s.CreatePartnerSource(PartnerSource{Name: name, AccountID: defaultAccountID, State: "PENDING"}))
		require.NoError(t, s.CreatePartnerEventBus(name, name, defaultAccountID))
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- s.SetPartnerSourceState(name, defaultAccountID, "ACTIVE") }()
		go func() { <-start; results <- s.DeleteEventBus(name, defaultAccountID) }()
		close(start)
		for j := 0; j < 2; j++ {
			err := <-results
			require.True(t, err == nil || errors.Is(err, ErrInvalidSourceState), "%v", err)
		}
		source, err := s.GetPartnerSource(name, defaultAccountID)
		require.NoError(t, err)
		require.Equal(t, "PENDING", source.State)
		require.Empty(t, source.BusName)
		_, err = s.GetEventBus(name, defaultAccountID)
		require.ErrorIs(t, err, ErrBusNotFound)
		require.NoError(t, s.DeletePartnerSource(name, defaultAccountID))
		require.NoError(t, s.CreatePartnerSource(PartnerSource{Name: name, AccountID: defaultAccountID, State: "PENDING"}))
		start = make(chan struct{})
		bound := make(chan error, 1)
		deleted := make(chan error, 1)
		go func() { <-start; bound <- s.CreatePartnerEventBus(name, name, defaultAccountID) }()
		go func() { <-start; deleted <- s.DeletePartnerSource(name, defaultAccountID) }()
		close(start)
		bindErr := <-bound
		require.NoError(t, <-deleted)
		_, err = s.GetPartnerSource(name, defaultAccountID)
		require.ErrorIs(t, err, ErrSourceNotFound)
		_, err = s.GetEventBus(name, defaultAccountID)
		if bindErr == nil {
			require.NoError(t, err)
			require.NoError(t, s.DeleteEventBus(name, defaultAccountID))
		} else {
			require.ErrorIs(t, bindErr, ErrSourceNotFound)
			require.ErrorIs(t, err, ErrBusNotFound)
		}
	}
}

func TestPartnerSourceDeletePreservesBus(t *testing.T) {
	s := newTestProvider(t).store
	const name = "aws.partner/devcloud/keep"
	require.NoError(t, s.CreatePartnerSource(PartnerSource{Name: name, AccountID: defaultAccountID, State: "PENDING"}))
	require.NoError(t, s.CreatePartnerEventBus(name, name, defaultAccountID))
	require.NoError(t, s.DeletePartnerSource(name, defaultAccountID))
	_, err := s.GetEventBus(name, defaultAccountID)
	require.NoError(t, err)
}
