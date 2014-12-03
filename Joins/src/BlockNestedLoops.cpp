#include "join.h"
#include "scan.h"

#include <vector>

//---------------------------------------------------------------
// BlockNestedLoop::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs a block nested loops join on the specified relations. 
// You can find a specification of this algorithm on page 455. You should 
// choose the smaller of the two relations to be the outer relation, but you 
// should make sure to concatenate the tuples in order <left, right> when 
// producing output. The block size can be specified in the constructor, 
// and is stored in the variable blockSize. 
//---------------------------------------------------------------
Status BlockNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);

	// Create temporary heapfile
	Status s;
	HeapFile *tmpHeap = new HeapFile(NULL, s);
	if (s != OK) {
		std::cerr << "Failed to create output heapfile." << std::endl;
		return FAIL;
	}

	// Open scan on left relation
	Status leftStatus;
	Scan *leftScan = left.file->OpenScan(leftStatus);
	if (leftStatus != OK) {
		std::cerr << "Failed to open scan on left relation." << std::endl;
		return FAIL;
	}

	// Create array to store block
	std::vector<char *> *block = new std::vector<char *>(this->blockSize);
	bool lastBlock = false;
	int lastRecPos = block->size(); // determines when to stop iterating through block if it is ever unfilled

	// Begin loop
	while (true) {
		if (lastBlock) break;

		// Read block into array
		for (int i = 0; i < this->blockSize; i++) {
			(*block)[i] = new char[left.recLen];		
			RecordID leftRid;
			leftStatus = leftScan->GetNext(leftRid, (*block)[i], left.recLen);
			if (leftStatus == DONE) {
				lastBlock = true;
				lastRecPos = i;
				break;
			}
			if (leftStatus != OK) return FAIL;
		}

		// Open scan on right relation
		Status rightStatus;
		Scan *rightScan = right.file->OpenScan(rightStatus);
		if (rightStatus != OK) {
			std::cerr << "Failed to open scan on right relation." << std::endl;
			return FAIL;
		}

		// Loop over right relation
		char *rightRec = new char[right.recLen];
		while (true) {
			RecordID rightRid;
			rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
			if (rightStatus == DONE) break;
			if (rightStatus != OK) return FAIL;

			// Join attribute on the right relation
			int *rightJoinValPtr = (int*)(rightRec + right.offset);
			
			// Compare current tuple from right relation to each tuple in array
			for (int i = 0; i < block->size(); i++) {
				if (i == lastRecPos) break;
				
				// Join attribute on the left relation
				int *leftJoinValPtr = (int*)((*block)[i] + left.offset);
				if (*leftJoinValPtr == *rightJoinValPtr) {
				// Create the record and insert into tmpHeap
					char *joinedRec = new char[out.recLen];
					MakeNewRecord(joinedRec, (*block)[i], rightRec, left, right);
					RecordID insertedRid;
					Status tmpStatus = tmpHeap->InsertRecord(joinedRec, out.recLen, insertedRid);
					
					if (tmpStatus != OK) {
						std::cerr << "Failed to insert tuple into output file." << std::endl;
						return FAIL;
					}

					delete [] joinedRec;
				}
			}
		}

		delete [] rightRec;
		delete rightScan;
	}

	out.file = tmpHeap;
	delete leftScan;
	delete block;

	return OK;
}


//Status BlockNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
//	JoinMethod::Execute(left, right, out);
//	
//
//	struct Tuple {
//		char *leftRec;
//		int *leftJoinValPtr;
//	};
//
//	// Create temporary heapfile
//	Status s;
//	HeapFile *tmpHeap = new HeapFile(NULL, s);
//	if (s != OK) {
//		std::cerr << "Failed to create output heapfile." << std::endl;
//		return FAIL;
//	}
//
//	// Open scan on left relation
//	Status leftStatus;
//	Scan *leftScan = left.file->OpenScan(leftStatus);
//	if (leftStatus != OK) {
//		std::cerr << "Failed to open scan on left relation." << std::endl;
//		return FAIL;
//	}
//
//
//	std::vector<Tuple *> block(this->blockSize);
//	bool lastBlock = false;
//	int cap = block.size();
//	//could create int for max position to go into block;
//
//	// Loop over the left relation
////	char *leftRec =nullptr;	
//	while (true) {
//
//		if (lastBlock) break;
//
//		// Create block from left relation
//		for (int i = 0; i < this->blockSize; i++) {
//			
//			Tuple *t = new Tuple();
//			t->leftRec = new char[left.recLen];
//			
//			RecordID leftRid;
//			leftStatus = leftScan->GetNext(leftRid, t->leftRec, left.recLen);
//			if (leftStatus == DONE) {
//				// might need to make new array of size i+1 and copy current contents;
//				// or make sure to clean array at end of right scan.
//				lastBlock = true;
//				cap = i;
//				break;
//			}
//			if (leftStatus != OK) return FAIL;
//
//			// Store join attribute from left relation
//
//			t->leftJoinValPtr = (int*)(t->leftRec + left.offset);;
//
//			block[i] = t;
//		}
//
//		// Open scan on right relation
//		Status rightStatus;
//		Scan *rightScan = right.file->OpenScan(rightStatus);
//		if (rightStatus != OK) {
//			std::cerr << "Failed to open scan on right relation." << std::endl;
//			return FAIL;
//		}
//
//		// Loop over right relation
//		char *rightRec = new char[right.recLen];
//		while (true) {
//			RecordID rightRid;
//			rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
//			if (rightStatus == DONE) break;
//			if (rightStatus != OK) return FAIL;
//
//			// Compare join attribute
//			int *rightJoinValPtr = (int*)(rightRec + right.offset);
//			
//			for (int i = 0; i < block.size(); i++) {
//				if (i == cap) break;
//
//				if (*(block[i]->leftJoinValPtr) == *rightJoinValPtr) {
//				// Create the record and insert into tmpHeap
//					char *joinedRec = new char[out.recLen];
//					// need to store leftRec somewhere in array
//					MakeNewRecord(joinedRec, block[i]->leftRec, rightRec, left, right);
//					RecordID insertedRid;
//					Status tmpStatus = tmpHeap->InsertRecord(joinedRec, out.recLen, insertedRid);
//					
//					if (tmpStatus != OK) {
//						std::cerr << "Failed to insert tuple into output file." << std::endl;
//						return FAIL;
//					}
//
//					delete [] joinedRec;
//				}
//			}
//		}
//
//		delete [] rightRec;
//		delete rightScan;
//	}
//
//	out.file = tmpHeap;
//	delete leftScan;
//
//
//	return OK;
//}
