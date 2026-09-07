// Phase 2 white-box election tests: drive RaftCore directly.
#include "raft/raft_core.h"

#include "harness/solo.h"
#include "tinytest.h"

using namespace raftkv;
using raftkv::sim::Solo;

static Message vote_req(NodeId from, uint64_t term, uint64_t lli, uint64_t llt) {
  Message m;
  m.type = MsgType::kRequestVote;
  m.from = from;
  m.term = term;
  m.last_log_index = lli;
  m.last_log_term = llt;
  return m;
}
static Message vote_resp(NodeId from, uint64_t term, bool granted) {
  Message m;
  m.type = MsgType::kRequestVoteResp;
  m.from = from;
  m.term = term;
  m.vote_granted = granted;
  return m;
}
static Message heartbeat(NodeId from, uint64_t term, uint64_t commit = 0) {
  Message m;
  m.type = MsgType::kAppendEntries;
  m.from = from;
  m.term = term;
  m.leader_commit = commit;
  return m;
}

TEST(election, single_node_self_elects) {
  Solo s(1, {1}, /*et=*/10, /*hb=*/2);
  CHECK_EQ(static_cast<int>(s.core().role()), static_cast<int>(Role::kFollower));
  s.tick(19);  // past the randomized [10,20) timeout
  CHECK(s.core().is_leader());
  CHECK_EQ(s.core().term(), 1u);
}

TEST(election, times_out_to_candidate_and_wins_with_majority) {
  Solo s(1, {1, 2, 3}, 10, 2);
  s.tick(19);
  CHECK_EQ(static_cast<int>(s.core().role()),
           static_cast<int>(Role::kCandidate));
  CHECK_EQ(s.core().term(), 1u);

  // it should have broadcast RequestVote to 2 and 3
  auto msgs = s.take_messages();
  int rv = 0;
  for (auto& m : msgs)
    if (m.type == MsgType::kRequestVote) ++rv;
  CHECK_EQ(rv, 2);

  s.step(vote_resp(2, 1, true));  // one grant -> majority (self + 2)
  CHECK(s.core().is_leader());
  CHECK_EQ(s.core().term(), 1u);
}

TEST(election, higher_term_message_steps_leader_down) {
  Solo s(1, {1, 2, 3}, 10, 2);
  s.tick(19);
  s.step(vote_resp(2, 1, true));
  REQUIRE(s.core().is_leader());

  s.step(heartbeat(2, /*term=*/5));  // a newer leader appeared
  CHECK_EQ(static_cast<int>(s.core().role()),
           static_cast<int>(Role::kFollower));
  CHECK_EQ(s.core().term(), 5u);
  CHECK_EQ(s.persisted_term(), 5u);
}

TEST(election, stale_term_request_vote_is_rejected) {
  Solo s(1, {1, 2, 3}, 10, 2);
  s.tick(19);              // term 1, voted for self
  s.step(vote_resp(2, 1, true));
  s.step(vote_resp(3, 1, true));
  REQUIRE(s.core().is_leader());
  s.take_messages();

  s.step(vote_req(2, /*term=*/1, 0, 0));  // stale term
  auto r = s.last_of(MsgType::kRequestVoteResp);
  REQUIRE(r.has_value());
  CHECK(!r->vote_granted);
  CHECK_EQ(r->term, s.core().term());
}

TEST(election, one_vote_per_term) {
  Solo s(1, {1, 2, 3}, 10, 2);
  // stay follower; receive a vote request from 2 at term 2
  s.step(vote_req(2, 2, 0, 0));
  auto r1 = s.last_of(MsgType::kRequestVoteResp);
  REQUIRE(r1.has_value());
  CHECK(r1->vote_granted);
  CHECK_EQ(s.persisted_vote(), 2u);
  s.take_messages();

  // 3 asks in the same term -> denied (already voted for 2)
  s.step(vote_req(3, 2, 0, 0));
  auto r2 = s.last_of(MsgType::kRequestVoteResp);
  REQUIRE(r2.has_value());
  CHECK(!r2->vote_granted);

  // re-asking by 2 in the same term is fine (idempotent)
  s.take_messages();
  s.step(vote_req(2, 2, 0, 0));
  auto r3 = s.last_of(MsgType::kRequestVoteResp);
  REQUIRE(r3.has_value());
  CHECK(r3->vote_granted);
}

TEST(election, vote_persists_across_restart) {
  Solo s(1, {1, 2, 3}, 10, 2);
  s.step(vote_req(2, 2, 0, 0));
  REQUIRE(s.last_of(MsgType::kRequestVoteResp)->vote_granted);

  s.restart();  // crash + recover from persisted HardState only

  CHECK_EQ(s.core().term(), 2u);
  s.step(vote_req(3, 2, 0, 0));  // different candidate, same term
  auto r = s.last_of(MsgType::kRequestVoteResp);
  REQUIRE(r.has_value());
  CHECK(!r->vote_granted);  // still remembers it voted for 2
}

TEST(election, up_to_date_check_denies_stale_candidate_log) {
  // Give node 1 a log: indices 1..3 at term 2 (via a leader's AppendEntries).
  Solo s(1, {1, 2, 3}, 10, 2);
  Message ae = heartbeat(2, 2);
  ae.prev_log_index = 0;
  ae.prev_log_term = 0;
  for (uint64_t i = 1; i <= 3; ++i)
    ae.entries.push_back(LogEntry{2, i, EntryType::kNormal, "x"});
  s.step(ae);
  REQUIRE(s.core().last_log_index() == 3u);

  // candidate with an older last term -> denied
  s.take_messages();
  s.step(vote_req(3, /*term=*/5, /*lli=*/9, /*llt=*/1));
  CHECK(!s.last_of(MsgType::kRequestVoteResp)->vote_granted);

  // candidate with same last term but shorter index -> denied
  s.take_messages();
  s.step(vote_req(3, /*term=*/6, /*lli=*/2, /*llt=*/2));
  CHECK(!s.last_of(MsgType::kRequestVoteResp)->vote_granted);

  // candidate at least as up-to-date -> granted
  s.take_messages();
  s.step(vote_req(3, /*term=*/7, /*lli=*/3, /*llt=*/2));
  CHECK(s.last_of(MsgType::kRequestVoteResp)->vote_granted);
}

TEST(election, candidate_times_out_and_bumps_term) {
  Solo s(1, {1, 2, 3}, 10, 2);
  s.tick(19);
  CHECK_EQ(s.core().term(), 1u);
  CHECK_EQ(static_cast<int>(s.core().role()),
           static_cast<int>(Role::kCandidate));
  s.take_messages();

  s.tick(19);  // no responses -> election times out again
  CHECK_EQ(s.core().term(), 2u);
  auto msgs = s.take_messages();
  int rv = 0;
  for (auto& m : msgs)
    if (m.type == MsgType::kRequestVote && m.term == 2) ++rv;
  CHECK_EQ(rv, 2);
}

TEST(election, heartbeat_resets_election_timer_no_spurious_candidacy) {
  Solo s(1, {1, 2, 3}, /*et=*/10, /*hb=*/2);
  for (int i = 0; i < 100; ++i) {
    s.tick(3);                 // 3 ticks
    s.step(heartbeat(2, 1));   // leader 2 keeps us alive
    CHECK_EQ(static_cast<int>(s.core().role()),
             static_cast<int>(Role::kFollower));
  }
  CHECK_EQ(s.core().term(), 1u);
}

TINYTEST_MAIN()
