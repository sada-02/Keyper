package balancer

var (
	// PrepareCommitDecisionKey is the key used to store the decision made by the balancer during a two-phase commit.
	PrepareCommitDecisionKey = []byte("PrepareCommitDecisionKey")
	// CoverageKey is the key used to store the Coverage of the balancer.
	CoverageKey = []byte("CoverageKey")
)
