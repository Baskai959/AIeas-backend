package app

import auctionapp "aieas_backend/internal/modules/auction/app"

type hammerDrainCoordinatorSet struct {
	items []auctionapp.HammerDrainCoordinator
}

func newHammerDrainCoordinatorSet(items ...auctionapp.HammerDrainCoordinator) auctionapp.HammerDrainCoordinator {
	out := make([]auctionapp.HammerDrainCoordinator, 0, len(items))
	for _, item := range items {
		if item != nil {
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return nil
	}
	if len(out) == 1 {
		return out[0]
	}
	return hammerDrainCoordinatorSet{items: out}
}

func (s hammerDrainCoordinatorSet) PendingForAuction(auctionID uint64) int {
	total := 0
	for _, item := range s.items {
		if item == nil {
			continue
		}
		if pending := item.PendingForAuction(auctionID); pending > 0 {
			total += pending
		}
	}
	return total
}
