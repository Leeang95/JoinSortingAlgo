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
	HeapFile *tmp = new HeapFile(NULL, s);
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


	std::vector<int *> block(this->blockSize);

	// Loop over the left relation
	char *leftRec = new char[left.recLen];
	bool *lastBlock = false;
	
	//could create int for max position to go into block;

	while (true) {
		if (lastBlock) break;

		// Create block from left relation
		for (int i=0; i<this->blockSize; i++) {
			RecordID leftRid;
			leftStatus = leftScan->GetNext(leftRid, leftRec, left.recLen);
			if (leftStatus == DONE) {
				// might need to make new array of size i+1 and copy current contents;
				// or make sure to clean array at end of right scan.
				*lastBlock = true;
				break;
			}
			if (leftStatus != OK) return FAIL;

			// Store join attribute from left relation
			int *leftJoinValPtr = (int*)(leftRec + left.offset);
			block[i] = leftJoinValPtr;
		}

		// Open scan on right relation
		char *rightRec = new char[right.recLen];
		while (true) {
		
		}


	}




	return s;
}