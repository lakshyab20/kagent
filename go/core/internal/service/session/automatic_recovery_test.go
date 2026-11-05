package session

import (
	"context"
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
)

func TestCrashedSessionSettlesTurnAndAcceptsNextMessage(t *testing.T) {
	for _, test := range []struct {
		name   string
		staged a2a.TaskState
		want   a2a.TaskState
	}{
		{name: "interrupted execution", want: a2a.TaskStateFailed},
		{name: "durable result before cleanup", staged: a2a.TaskStateCompleted, want: a2a.TaskStateCompleted},
		{name: "uncheckpointed input wait", staged: a2a.TaskStateInputRequired, want: a2a.TaskStateFailed},
		{name: "uncheckpointed auth wait", staged: a2a.TaskStateAuthRequired, want: a2a.TaskStateFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, session := lifecycleFixture(t)
			actors := &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}
			workflow := NewActorWorkflow(store, actors, make(chan struct{}, 1), 10*time.Millisecond, true)
			session, err := workflow.Create(t.Context(), session)
			require.NoError(t, err)
			message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello"))
			message.ContextID = session.ContextId
			task := a2a.NewSubmittedTask(message, message)
			task.Status.State = a2a.TaskStateWorking
			hash := sha256.Sum256([]byte("working"))
			version, err := store.CreateRuntimeTask(t.Context(), session.Id, hash[:], task, "")
			require.NoError(t, err)
			if test.staged != "" {
				task.Status.State = test.staged
				hash = sha256.Sum256([]byte("completed"))
				_, err = store.UpdateSessionTask(t.Context(), session.Id, version, hash[:], task, task, "")
				require.NoError(t, err)
			}
			key := actorKey("team-a", substrate.ActorName(session.Id))
			actors.actors[key].Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
			actors.actors[key].Status.Snapshots = []*ateapipb.Snapshot{{Generation: 1, Fidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, DurableSnapshot: &ateapipb.SnapshotStorage{State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "s3://snapshots/previous-turn"}}}}
			before := proto.CloneOf(actors.actors[key])
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- workflow.Start(ctx) }()
			t.Cleanup(func() { cancel(); require.NoError(t, <-done) })

			require.EventuallyWithT(t, func(t *assert.CollectT) {
				stored, err := store.GetSettledSessionTask(ctx, session.Id, string(task.ID), nil)
				if !assert.NoError(t, err) {
					return
				}
				assert.Equal(t, test.want, stored.Status.State)
				current, err := store.GetSessionByID(ctx, session.Id)
				if assert.NoError(t, err) {
					assert.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, current.State)
				}
			}, 5*time.Second, 10*time.Millisecond)
			after, err := actors.GetActor(ctx, "team-a", substrate.ActorName(session.Id))
			require.NoError(t, err)
			require.Equal(t, before.Metadata.Uid, after.Metadata.Uid)
			require.True(t, proto.Equal(substrate.LatestDurableActorSnapshot(before), substrate.LatestDurableActorSnapshot(after)))
			require.Equal(t, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, after.Status.State)
			current, err := store.GetSessionByID(ctx, session.Id)
			require.NoError(t, err)
			require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, current.State)
			require.Equal(t, session.PreparedRevision, current.PreparedRevision)
			checkpoint := &apiv1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: string(task.ID)}
			_, _, err = store.ReserveSessionCheckpoint(ctx, checkpoint, session.Creator, uuid.NewString())
			require.ErrorIs(t, err, database.ErrSnapshotPending, "the older Actor snapshot does not checkpoint the discarded turn")
			require.NoError(t, store.ReserveSessionDispatch(ctx, session.Id, uuid.New(), uuid.NewString()))
		})
	}
}

// crashedFleet creates count READY Sessions beside fixture's and returns every ID
// in order. Each has an accepted turn its runtime was executing, and every runtime
// starts healthy so each test chooses which crash.
func crashedFleet(t *testing.T, store *lifecycleTestStore, actors *lifecycleTestActors, first *apiv1.Session, count int) []string {
	t.Helper()
	actors.actors[actorKey("team-a", substrate.ActorName(first.Id))].Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	ids := []string{first.Id}
	for range count {
		session, _, err := store.CreateSession(t.Context(), &apiv1.Session{
			Id: uuid.NewString(), Creator: first.Creator, Agent: first.Agent,
		}, uuid.NewString())
		require.NoError(t, err)
		session, err = NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Minute, false).Create(t.Context(), session)
		require.NoError(t, err)
		message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("accepted turn"))
		message.ContextID = session.ContextId
		task := a2a.NewSubmittedTask(message, message)
		task.Status.State = a2a.TaskStateWorking
		hash := sha256.Sum256([]byte(task.ID))
		_, err = store.CreateRuntimeTask(t.Context(), session.Id, hash[:], task, "")
		require.NoError(t, err)
		ids = append(ids, session.Id)
	}
	slices.Sort(ids)
	return ids
}

func actorState(t *testing.T, actors *lifecycleTestActors, id string) ateapipb.ActorState {
	t.Helper()
	actor, err := actors.GetActor(t.Context(), "team-a", substrate.ActorName(id))
	require.NoError(t, err)
	return actor.GetStatus().GetState()
}

// One sweep reaches every crash, wherever it sorts, with Substrate reads that grow
// with Actor pages and crashes rather than with READY Sessions.
func TestAutomaticRecoveryFindsEveryCrashInOneSweep(t *testing.T) {
	store, actors, first, _, _ := crashedTurnFixture(t)
	ids := crashedFleet(t, store, actors, first, 149)
	crashed := []string{ids[0], ids[75], ids[149]}
	for _, id := range crashed {
		actors.actors[actorKey("team-a", substrate.ActorName(id))].Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
	}
	// Crashed Actors that no live Session records never nominate recovery.
	stray := uuid.NewString()
	for _, name := range []string{"sandbox-" + stray, substrate.ActorName(stray), "session-not-a-uuid"} {
		actors.actors[actorKey("team-a", name)] = &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: name, Uid: "actor-uid"},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
		}
	}
	actors.listPageSize, actors.listCalls, actors.reads = 20, 0, 0

	workflow := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Minute, true)
	require.NoError(t, workflow.recoverCrashedActors(t.Context()))
	listCalls, reads := actors.listCalls, actors.reads

	for _, id := range crashed {
		require.Equal(t, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, actorState(t, actors, id))
		current, err := store.GetSessionByID(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, apiv1.RuntimeState_RUNTIME_STATE_READY, current.State)
	}
	require.Equal(t, 8, listCalls, "153 Actors in pages of 20")
	require.Equal(t, 2*len(crashed), reads, "only crashed Actors are read individually, once to admit and once to revert")
	require.Equal(t, ateapipb.ActorState_ACTOR_STATE_CRASHED, actorState(t, actors, stray), "an unrecorded Actor is left alone")
}

func TestAutomaticRecoveryContinuesPastFailures(t *testing.T) {
	store, base, first, _, _ := crashedTurnFixture(t)
	ids := crashedFleet(t, store, base, first, 2)
	for _, id := range ids {
		base.actors[actorKey("team-a", substrate.ActorName(id))].Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
	}
	failing := substrate.ActorName(ids[0])
	actors := &recoveryTestActors{lifecycleTestActors: base, revert: func(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
		if name == failing {
			return nil, status.Error(codes.Unavailable, "control plane unavailable")
		}
		return base.RevertActor(ctx, atespace, name)
	}}
	workflow := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Minute, true)

	base.listErr = status.Error(codes.Unavailable, "control plane unavailable")
	require.ErrorContains(t, workflow.recoverCrashedActors(t.Context()), "atespace team-a")
	for _, id := range ids {
		require.Equal(t, ateapipb.ActorState_ACTOR_STATE_CRASHED, actorState(t, base, id), "a failed list admits nothing")
	}

	base.listErr = nil
	require.NoError(t, workflow.recoverCrashedActors(t.Context()))
	require.Equal(t, ateapipb.ActorState_ACTOR_STATE_CRASHED, actorState(t, base, ids[0]))
	for _, id := range ids[1:] {
		require.Equal(t, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, actorState(t, base, id), "one failed revert does not stop the sweep")
	}
	admitted, err := store.ListSessionRecoveryCandidates(t.Context(), "", 100)
	require.NoError(t, err)
	require.Len(t, admitted, 1, "the failed revert stays admitted and retries from PostgreSQL")
	require.Equal(t, ids[0], admitted[0].Id)

	actors.revert = base.RevertActor
	after, err := workflow.recoverSessionPage(t.Context(), "")
	require.NoError(t, err)
	require.Empty(t, after)
	require.Equal(t, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, actorState(t, base, ids[0]))
}

func TestAutomaticRecoveryCandidatePolicy(t *testing.T) {
	store, actors, active, _, _ := crashedTurnFixture(t)
	workflow := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Second, false)
	for _, state := range []a2a.TaskState{"", a2a.TaskStateInputRequired, a2a.TaskStateAuthRequired, a2a.TaskStateWorking} {
		session, _, err := store.CreateSession(t.Context(), &apiv1.Session{Id: uuid.NewString(), Creator: "alice", Agent: active.Agent}, uuid.NewString())
		require.NoError(t, err)
		session, err = workflow.Create(t.Context(), session)
		require.NoError(t, err)
		if state == "" {
			continue
		}
		message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("input"))
		message.ContextID = session.ContextId
		task := a2a.NewSubmittedTask(message, message)
		task.Status.State = state
		hash := sha256.Sum256([]byte(task.ID))
		version, err := store.CreateRuntimeTask(t.Context(), session.Id, hash[:], task, "")
		require.NoError(t, err)
		if state != a2a.TaskStateWorking {
			require.NoError(t, store.SettleSessionTask(t.Context(), session.Id, string(task.ID), version))
		}
	}
	atespaces, err := store.ListSessionRecoveryAtespaces(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"team-a"}, atespaces, "idle, waiting and active READY Sessions are all swept")
	page, err := store.ListSessionRecoveryCandidates(t.Context(), "", 100)
	require.NoError(t, err)
	require.Empty(t, page, "READY Sessions are never retried from PostgreSQL")
	actor, err := actors.GetActor(t.Context(), "team-a", substrate.ActorName(active.Id))
	require.NoError(t, err)
	_, err = store.BeginCrashedSessionRecovery(t.Context(), active.Id, revertGeneration(t, store, active.Id), actor.GetMetadata().GetUid(), database.SessionTaskSnapshot{})
	require.NoError(t, err)
	page, err = store.ListSessionRecoveryCandidates(t.Context(), "", 100)
	require.NoError(t, err)
	require.Len(t, page, 1, "admitted recovery still converges after automatic recovery is disabled")
	require.Equal(t, active.Id, page[0].Id)
}

func TestAutomaticRecoveryIsOptInForIdleSessions(t *testing.T) {
	store, session := lifecycleFixture(t)
	actors := &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}}
	manual := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Minute, false)
	session, err := manual.Create(t.Context(), session)
	require.NoError(t, err)
	key := actorKey("team-a", substrate.ActorName(session.Id))
	actors.actors[key].Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
	actors.listCalls = 0
	manual.scanRecovery(t.Context(), "")
	require.Equal(t, ateapipb.ActorState_ACTOR_STATE_CRASHED, actorState(t, actors, session.Id))
	require.Zero(t, actors.listCalls, "disabled automatic recovery does not list Actors")
	observed, err := manual.Recovery(t.Context(), session)
	require.NoError(t, err)
	require.True(t, observed.GetNeeded())
	require.False(t, observed.Automatic)

	automatic := NewActorWorkflow(store, actors, make(chan struct{}, 1), time.Minute, true)
	automatic.scanRecovery(t.Context(), "")
	require.Equal(t, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, actorState(t, actors, session.Id), "opt-in recovery includes an idle crashed runtime")
	observed, err = automatic.Recovery(t.Context(), session)
	require.NoError(t, err)
	require.True(t, observed.Automatic)
	require.False(t, observed.GetNeeded())
}
