package sig

type Sig struct {
	listerner []*chan string
	Close     chan string
}

func NewStringSig() *Sig {
	return &Sig{
		Close: make(chan string, 1),
	}
}

func (sig *Sig) Send(control string) {
	for _, i := range sig.listerner {
		*i <- control
	}
}

func (sig *Sig) Recv() *chan string {
	ch := make(chan string, 1)
	sig.listerner = append(sig.listerner, &ch)
	return &ch
}

func (sig *Sig) Clean(ch *chan string) {
	var (
		num = -1
		l   = sig.listerner
	)
	for n, i := range l {
		if i == ch {
			num = n
			break
		}
	}
	if num != -1 {
		// replace num's with first
		l[num] = l[0]
		// remove first
		sig.listerner = l[1:]
	}
	if len(sig.listerner) == 0 {
		sig.Close <- ""
	}
}
