#include "join.h"
#include "scan.h"
#include "bufmgr.h"
#include "BTreeFile.h"
#include "BTreeFileScan.h"

//---------------------------------------------------------------
// IndexNestedLoops::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs an index nested loops join on the specified relations. 
// You should create a BTreeFile index on the join attribute of the right 
// relation, and then probe it for each record in the left relation. Remember 
// that the BTree expects string keys, so you will have to convert the integer
// attributes to a string using JoinMethod::toString. Note that the join may 
// not be a foreign key join, so there may be multiple records indexed by the 
// same key. Good thing our B-Tree supports this! Don't forget to destroy the 
// BTree when you are done. 
//---------------------------------------------------------------
Status IndexNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);

	// Make sure left relation is smaller, if not need to swap JoinSpecs
	bool swapped = false;
	if (left.file->GetNumOfRecords() > right.file->GetNumOfRecords()) {
		JoinSpec tmp = left;
		left = right;
		right = tmp;

		swapped = true;
	}

	// Create temporary heapfile
	Status s;
	HeapFile *tmpHeap = new HeapFile(NULL, s);
	if (s != OK) {
		std::cerr << "Failed to create output heapfile." << std::endl;
		return FAIL;
	}
	
	// Create BTreeFileIndex
	BTreeFile *bTree = new BTreeFile(s, "BTREE");
	if (s != OK) {
		std::cerr << "Failed to create BTreeFile." << std::endl;
		return FAIL;
	}

	// Open scan on right relation
	Status rightStatus;
	Scan *rightScan = right.file->OpenScan(rightStatus);
	if (rightStatus != OK) {
		std::cerr << "Failed to open scan on right relation." << std::endl;
		return FAIL;
	}

	// Loop over right relation and fill btree
	char *rightRec = new char[right.recLen];
	while (true) {
		RecordID rightRid;
		rightStatus = rightScan->GetNext(rightRid, rightRec, right.recLen);
		if (rightStatus == DONE) break;
		if (rightStatus != OK) return FAIL;

		// The join attribute on the righht relation.
		int *rightJoinValPtr = (int*)(rightRec + right.offset);
		
		char *keyToInsert = new char[right.recLen+1];
		this->toString(*rightJoinValPtr,keyToInsert);
		bTree->Insert(keyToInsert, rightRid);
	}

	delete rightScan;

	// Open scan on left relation
	Status leftStatus;
	Scan *leftScan = left.file->OpenScan(leftStatus);
	if (leftStatus != OK) {
		std::cerr << "Failed to open scan on left relation" << std::endl;
		return FAIL;
	}

	// Loop over left relation
	char *leftRec = new char[left.recLen];
	while (true) {
		RecordID leftRid;
		leftStatus = leftScan->GetNext(leftRid, leftRec, left.recLen);
		if (leftStatus == DONE) break;
		if (leftStatus != OK) return FAIL;

		// Search btree for possible mathes on join attribute
		int *leftJoinValPtr = (int*)(leftRec + left.offset);
		char *key = new char[left.recLen + 1];
		this->toString(*leftJoinValPtr, key);
		BTreeFileScan *btScan = bTree->OpenScan(key, key);

		// Loop through matched attributes
		while (true) {
			char *tmpKey = NULL;
			RecordID rid;
			Status bTreeStatus = btScan->GetNext(rid, tmpKey);
			if (bTreeStatus == DONE) break;
			if (bTreeStatus != OK) {
				std::cerr << "Failed during scan." << std::endl;
				return FAIL;
			}

			// Find page with matched rid and create new record
			if (bTreeStatus == OK) {
				Status searchStatus;
				Page *hPage;
				char *rightRec = new char[right.recLen];
				PIN (rid.pageNo, hPage);
				searchStatus = ((HeapPage*)hPage)->ReturnRecord(rid, rightRec, right.recLen);
				// Btree gave a page that does not hold the given rid
				if (searchStatus != OK) {
					std::cerr << "BTree holds incorrect data." << std::endl;
					UNPIN (rid.pageNo, CLEAN);
					return FAIL;
				}
				// Create new record and insert into tmpHeap
				char *joinedRec = new char[out.recLen];

				// Need to check if JoinSpecs have been swapped
					if (swapped) 
						MakeNewRecord(joinedRec, rightRec, leftRec, right, left);
					else
						MakeNewRecord(joinedRec, leftRec, rightRec, left, right);

				RecordID insertedRid;
				Status tmpStatus = tmpHeap->InsertRecord(joinedRec, out.recLen, insertedRid);
				if (tmpStatus != OK) {
					std::cerr << "Failed to insert tuple into output heapfile." << std::endl;
					UNPIN (rid.pageNo, CLEAN);
					return FAIL;
				}

				delete [] joinedRec;
				UNPIN (rid.pageNo, CLEAN);
			}
		}

		delete btScan;
	}

	out.file = tmpHeap;
	delete leftScan;
	delete [] leftRec;
	bTree->DestroyFile();
	delete bTree;

	return OK;
}