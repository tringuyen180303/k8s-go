package api

// Object is one Pod, Node, or ReplicaSet stored by the registry.
type Object interface {
	GetObjectMeta() *ObjectMeta
	Kind() string
}

func (p *Pod) GetObjectMeta() *ObjectMeta { return &p.Metadata }
func (p *Pod) Kind() string               { return "Pod" }

func (n *Node) GetObjectMeta() *ObjectMeta { return &n.Metadata }
func (n *Node) Kind() string               { return "Node" }

func (l *Lease) GetObjectMeta() *ObjectMeta { return &l.Metadata }
func (l *Lease) Kind() string               { return "Lease" }

func (l *LeaderLease) GetObjectMeta() *ObjectMeta { return &l.Metadata }
func (l *LeaderLease) Kind() string               { return "LeaderLease" }

func (r *ReplicaSet) GetObjectMeta() *ObjectMeta { return &r.Metadata }
func (r *ReplicaSet) Kind() string               { return "ReplicaSet" }

func (s *Service) GetObjectMeta() *ObjectMeta { return &s.Metadata }
func (s *Service) Kind() string               { return "Service" }

func (e *Endpoints) GetObjectMeta() *ObjectMeta { return &e.Metadata }
func (e *Endpoints) Kind() string               { return "Endpoints" }
