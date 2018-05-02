package sig

type Sig struct {
	listerner []chan string
}

func NewStringSig() *Sig {
	return &Sig{}
}

func (sig Sig) Send(control string) {
	for _, i := range sig.listerner {
		go func(control string) {
			i <- control
		}(control)
	}
}

func (sig Sig) Recv() *chan string {
	ch := make(chan string)
	sig.listerner = append(sig.listerner, ch)
	return &ch
}

func (sig Sig) Clean(ch *chan string) {
	var (
		num = -1
		l   = sig.listerner
	)
	for n, i := range l {
		if i == *ch {
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
}
