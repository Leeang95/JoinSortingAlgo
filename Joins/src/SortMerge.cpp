#include "join.h"
#include "scan.h"


//---------------------------------------------------------------
// SortMerge::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs an sort merge join on the specified relations. 
// Please see the pseudocode on page 460 of your text for more info
// on this algorithm. You may use JoinMethod::SortHeapFile to sort
// the relations. 
//---------------------------------------------------------------
Status SortMerge::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);

	// Make sure left relation is smaller, if not need to swap JoinSpecs
	bool swapped = false;
	if (left.file->GetNumOfRecords() > right.file->GetNumOfRecords()) {
		JoinSpec tmp = left;
		left = right;
		right = tmp;

		swapped = true;
	}

	// Create the temporary heapfile
	Status s;
	HeapFile *tmpHeap = new HeapFile(NULL, s);
	if (s != OK) {
		std::cerr << "Failed to create output heapfile." << std::endl;
		return FAIL;
	}

	// Need to sort relations
	HeapFile *sortedLeft = JoinMethod::SortHeapFile(left.file, left.recLen, left.offset);
	HeapFile *sortedRight = JoinMethod::SortHeapFile(right.file, right.recLen, right.offset);

	// Open scan on sorted left relation
	Status sortedLeftStatus;
	Scan *sortedLeftScan = sortedLeft->OpenScan(sortedLeftStatus);
	if (sortedLeftStatus != OK) {
		std::cerr << "Failed to open scan on sorted left relation" << std::endl;
		return FAIL;
	}

	// Open scan on sorted right relation
	Status sortedRightStatus;
	Scan *sortedRightScan = sortedRight->OpenScan(sortedRightStatus);
	if (sortedRightStatus != OK) {
		std::cerr << "Failed to open scan on sorted right relation." << std::endl;
		return FAIL;
	}

	// Get first elements of each relation.
	char *sortedLeftRec = new char [left.recLen];
	RecordID sortedLeftRid;
	sortedLeftStatus = sortedLeftScan->GetNext(sortedLeftRid, sortedLeftRec, left.recLen);
	if (sortedLeftStatus == DONE) return FAIL; // Left relation was empty
	if (sortedLeftStatus != OK) return FAIL;
	int *tR = (int*)(sortedLeftRec + left.offset);

	char *sortedRightRec = new char[right.recLen];
	RecordID sortedRightRid;
	sortedRightStatus = sortedRightScan->GetNext(sortedRightRid, sortedRightRec, right.recLen);
	if (sortedRightStatus == DONE) return FAIL; // Right relation was empty
	if (sortedRightStatus != OK) return FAIL;
	int *tS = (int*)(sortedRightRec + right.offset);

	// Pointer to start of each partition
	RecordID partitionRid;
	int *tG = new int();
	*tG = *tS;

	// Keep scanning as long as we still can
	while (sortedLeftStatus == OK && sortedRightStatus == OK) {
		// Continue to scan R
		while (*tR < *tG) {
			sortedLeftStatus = sortedLeftScan->GetNext(sortedLeftRid, sortedLeftRec, left.recLen);
			if (sortedLeftStatus == DONE) break;
			if (sortedLeftStatus != OK) return FAIL;
			tR = (int*)(sortedLeftRec + left.offset);
		}
		// Continue to scan S
		while (*tR > *tG) {
			sortedRightStatus = sortedRightScan->GetNext(sortedRightRid, sortedRightRec, right.recLen);
			if (sortedRightStatus == DONE) break;
			if (sortedRightStatus != OK) return FAIL;
			tG = (int*)(sortedRightRec + right.offset);
		}

		*tS = *tG;
		partitionRid = sortedRightRid;
		
		while (*tR == *tG) {
			// Reset partition iterator tS
			*tS = *tG;
			while (*tS == *tR) {
				//	Create the record and insert into tmpHeap...
				char *joinedRec = new char[out.recLen];
				
				// Need to check if JoinSpecs have been swapped
					if (swapped) 
						MakeNewRecord(joinedRec, sortedRightRec, sortedLeftRec, right, left);
					else
						MakeNewRecord(joinedRec, sortedLeftRec, sortedRightRec, left, right);

				RecordID insertedRid;
				Status tmpStatus = tmpHeap->InsertRecord(joinedRec, out.recLen, insertedRid);

				if (tmpStatus != OK) {
					std::cerr << "Failed to insert tuple into output heapfile." << std::endl;
					return FAIL;
				}
				delete [] joinedRec;

				sortedRightStatus = sortedRightScan->GetNext(sortedRightRid, sortedRightRec, right.recLen);
				if (sortedRightStatus == DONE) break; //{sortedRightStatus = OK; break;}
				if (sortedRightStatus != OK) return FAIL;
				tS = (int*)(sortedRightRec + right.offset);
			}

			// Setup next tuple from left relation
			sortedLeftStatus = sortedLeftScan->GetNext(sortedLeftRid, sortedLeftRec, left.recLen);
			if (sortedLeftStatus == DONE) break;
			if (sortedLeftStatus != OK) return FAIL;
			tR = (int*)(sortedLeftRec + left.offset);

			// Setup pointer to last known partition for new tuple from left relation
			*tS = *tG;
			sortedRightScan->MoveTo(partitionRid);
			sortedRightStatus = sortedRightScan->GetNext(sortedRightRid, sortedRightRec, right.recLen);
		}

		// Initialize search for next partition
		*tG = *tS;
		sortedRightScan->MoveTo(sortedRightRid);
		if (sortedRightStatus != DONE) 
			sortedRightStatus = sortedRightScan->GetNext(sortedRightRid, sortedRightRec, right.recLen);
	}

	out.file = tmpHeap;
	delete sortedLeftScan;
	delete [] sortedLeftRec;

	delete sortedRightScan;
	delete [] sortedRightRec;

	return OK;
}