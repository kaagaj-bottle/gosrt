package live

import (
	"testing"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
)

func bufferedPacket(sequence uint32, size int) packet.Packet {
	p := packet.NewPacket(nil)
	p.SetData(make([]byte, size))
	p.Header().PacketSequenceNumber = circular.New(sequence, packet.MAX_SEQUENCENUMBER)
	p.Header().PktTsbpdTime = 1000000000
	return p
}

func TestReceiverBufferLimit(t *testing.T) {
	for _, size := range []int{1024, 0} {
		calls := 0
		r := NewReceiver(ReceiveConfig{InitialSequenceNumber: circular.New(0, packet.MAX_SEQUENCENUMBER), MaxBufferBytes: 2048, OnBufferFull: func() { calls++ }}).(*receiver)
		for i := range 1000 {
			r.Push(bufferedPacket(uint32(i), size))
		}
		wantPackets := 16
		if size != 0 {
			wantPackets = 2048 / size
		}
		if r.statistics.ByteBuf != uint64(wantPackets*size) || r.packetList.Len() != wantPackets || calls != 1 {
			t.Fatalf("size=%d bytes=%d packets=%d callbacks=%d", size, r.statistics.ByteBuf, r.packetList.Len(), calls)
		}
	}
}

func TestSenderLimitIncludesUnacknowledgedPackets(t *testing.T) {
	calls := 0
	s := NewSender(SendConfig{InitialSequenceNumber: circular.New(0, packet.MAX_SEQUENCENUMBER), MaxBufferBytes: 2048, DropThreshold: 1000000, OnBufferFull: func() { calls++ }}).(*sender)
	s.Push(bufferedPacket(0, 1024))
	s.Push(bufferedPacket(1, 1024))
	s.Tick(1000000000)
	if s.lossList.Len() != 2 {
		t.Fatal("expected unacknowledged packets")
	}
	for i := range 1000 {
		s.Push(bufferedPacket(uint32(i), 1024))
	}
	if s.statistics.ByteBuf != 2048 || s.packetList.Len() != 0 || calls != 1 {
		t.Fatalf("bytes=%d queued=%d callbacks=%d", s.statistics.ByteBuf, s.packetList.Len(), calls)
	}
}

func TestSenderLimitBoundsEmptyPackets(t *testing.T) {
	calls := 0
	s := NewSender(SendConfig{InitialSequenceNumber: circular.New(0, packet.MAX_SEQUENCENUMBER), MaxBufferBytes: 2048, OnBufferFull: func() { calls++ }}).(*sender)
	for i := range 1000 {
		s.Push(bufferedPacket(uint32(i), 0))
	}
	if s.packetList.Len() != 16 || calls != 1 {
		t.Fatalf("packets=%d callbacks=%d", s.packetList.Len(), calls)
	}
}

func TestBufferLimitZeroIsUnlimited(t *testing.T) {
	calls := 0
	initial := circular.New(0, packet.MAX_SEQUENCENUMBER)
	r := NewReceiver(ReceiveConfig{InitialSequenceNumber: initial, OnBufferFull: func() { calls++ }}).(*receiver)
	s := NewSender(SendConfig{InitialSequenceNumber: initial, OnBufferFull: func() { calls++ }}).(*sender)
	for i := range 1000 {
		r.Push(bufferedPacket(uint32(i), 1024))
		s.Push(bufferedPacket(uint32(i), 1024))
	}
	if r.statistics.ByteBuf != 1000*1024 || r.packetList.Len() != 1000 ||
		s.statistics.ByteBuf != 1000*1024 || s.packetList.Len() != 1000 || calls != 0 {
		t.Fatalf("receive bytes=%d packets=%d; send bytes=%d packets=%d; callbacks=%d",
			r.statistics.ByteBuf, r.packetList.Len(), s.statistics.ByteBuf, s.packetList.Len(), calls)
	}
}

func TestSenderBufferLimitReleasesAcknowledgedPackets(t *testing.T) {
	calls := 0
	s := NewSender(SendConfig{
		InitialSequenceNumber: circular.New(0, packet.MAX_SEQUENCENUMBER),
		MaxBufferBytes:        2048,
		DropThreshold:         1000000,
		OnBufferFull:          func() { calls++ },
	}).(*sender)
	s.Push(bufferedPacket(0, 1024))
	s.Push(bufferedPacket(1, 1024))
	s.Tick(1000000000)
	s.ACK(circular.New(1, packet.MAX_SEQUENCENUMBER))
	s.Push(bufferedPacket(2, 1024))
	if s.statistics.ByteBuf != 2048 || s.lossList.Len() != 1 || s.packetList.Len() != 1 || calls != 0 {
		t.Fatalf("bytes=%d unacknowledged=%d queued=%d callbacks=%d",
			s.statistics.ByteBuf, s.lossList.Len(), s.packetList.Len(), calls)
	}
}
