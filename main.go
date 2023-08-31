package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	ID               int
	Accounts         []int
	MsgChannels      []chan Transaction
	Snapshot         []int
	State            []int
	Lock             sync.Mutex
	wg               sync.WaitGroup
	markerWg         sync.WaitGroup
	transactionCount int
}

type Transaction struct {
	Amount int
	Source int
	Marker bool
}

var numNodes int
var nodes []Node
var totalAmount int
var totalLock sync.Mutex

const TransactionsPerSnapshot = 15

func generateTransaction(nodeID, numNodes int) (destinationID, amount int) {
	destinationID = rand.Intn(numNodes)
	amount = rand.Intn(100)
	return destinationID, amount
}

func (n *Node) performTransaction() {
	defer n.wg.Done()
	for i := 0; i < TransactionsPerSnapshot; i++ {
		time.Sleep(time.Millisecond * 500)
		destinationID, amount := generateTransaction(n.ID, numNodes)

		if destinationID != n.ID {
			n.Lock.Lock()
			n.Accounts[destinationID] += amount
			n.Accounts[n.ID] -= amount
			n.Lock.Unlock()

			fmt.Printf("Node %d sent $%d to Node %d\n", n.ID, amount, destinationID)
			totalLock.Lock()
			totalAmount += amount
			totalLock.Unlock()

			n.transactionCount++

			if n.transactionCount%TransactionsPerSnapshot == 0 {
				n.initiateSnapshot()
			}
		}
	}
}

func (n *Node) initiateSnapshot() {
	n.Lock.Lock()
	n.Snapshot = make([]int, numNodes)
	copy(n.Snapshot, n.Accounts)
	for i := 0; i < numNodes; i++ {
		if i != n.ID {
			n.MsgChannels[i] <- Transaction{Source: n.ID, Marker: true}
		}
	}
	n.Lock.Unlock()
}

func (n *Node) handleMarkerMessages() {
	defer n.markerWg.Done()
	receivedMarkers := make([]bool, numNodes)
	for {
		msg, ok := <-n.MsgChannels[n.ID]
		if !ok {
			break
		}
		if msg.Marker {
			n.Lock.Lock()
			if !receivedMarkers[msg.Source] {
				n.State[msg.Source] = n.Accounts[msg.Source]
				receivedMarkers[msg.Source] = true
				n.handleSnapshotMessages()
			}
			n.Lock.Unlock()
		}
	}
}

func (n *Node) handleSnapshotMessages() {
	for i := 0; i < numNodes; i++ {
		if i != n.ID {
			n.MsgChannels[i] <- Transaction{Source: n.ID, Amount: n.Snapshot[n.ID]}
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	fmt.Print("Enter the number of nodes: ")
	_, err := fmt.Scan(&numNodes)
	if err != nil {
		fmt.Println("Error reading input:", err)
		return
	}

	nodes = make([]Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = Node{
			ID:          i,
			Accounts:    make([]int, numNodes),
			MsgChannels: make([]chan Transaction, numNodes),
			Snapshot:    make([]int, numNodes),
			State:       make([]int, numNodes),
		}
		for j := 0; j < numNodes; j++ {
			nodes[i].MsgChannels[j] = make(chan Transaction, 100)
		}
		for j := 0; j < numNodes; j++ {
			nodes[i].Accounts[j] = rand.Intn(1000) + 900
		}
		fmt.Printf("Node %d initial amount: $%d\n", i, calculateSum(nodes[i].Accounts))
		totalLock.Lock()
		totalAmount += calculateSum(nodes[i].Accounts)
		totalLock.Unlock()
		nodes[i].wg.Add(1)
		nodes[i].markerWg.Add(1)
		go nodes[i].performTransaction()
		go nodes[i].handleMarkerMessages()
	}
	fmt.Printf("Total amount in the entire system: $%d\n", totalAmount)
	for i := 0; i < numNodes; i++ {
		nodes[i].wg.Wait()
	}
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			close(nodes[i].MsgChannels[j])
		}
	}
	for i := 0; i < numNodes; i++ {
		nodes[i].markerWg.Wait()
	}
	fmt.Printf("\nSnapshots and Consistency Check:\n")
	for i, n := range nodes {
		fmt.Printf("Snapshot for Node %d: %v\n", i, n.Snapshot)
		isConsistent := true
		for j, snapshotAmount := range n.Snapshot {
			if snapshotAmount != n.State[j] {
				isConsistent = false
				break
			}
		}
		fmt.Printf("Is Snapshot Consistent for Node %d: %v\n", i, isConsistent)
		snapshotTotal := calculateSum(n.Snapshot)
		fmt.Printf("Total amount in snapshot for Node %d: $%d\n", i, snapshotTotal)
		if snapshotTotal == totalAmount {
			fmt.Printf("Snapshot total matches initial total for Node %d\n", i)
		} else {
			fmt.Printf("Snapshot total does not match initial total for Node %d\n", i)
		}
	}
}

func calculateSum(nums []int) int {
	total := 0
	for _, num := range nums {
		total += num
	}
	return total
}
