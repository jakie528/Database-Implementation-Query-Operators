#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */


const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	Status status;
	
	int reclen = 0;
	AttrDesc projNamesArray[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         projNamesArray[i]);
        if (status != OK){ return status;}

		reclen += projNamesArray[i].attrLen;
    }
	AttrDesc *attrDesc = new AttrDesc();
    if (attr != NULL){
    status = attrCat->getInfo(attr->relName,
                                     attr->attrName,
                                     *attrDesc);
    if (status != OK){ return status; }
    }
    
    int type = attrDesc->attrType;

    const char *attrValue_2;

    switch(type) {
    case INTEGER:{
        int new_val = atoi(attrValue);
        attrValue_2 = (char *)&new_val;
        break;
    }
    case FLOAT:{
        float new_val = atof(attrValue);
        attrValue_2 = (char *)&new_val;
        break;
    }
    case STRING:{
        attrValue_2 = attrValue;
        break;
    }
    default:
        break;
    }

	
	return ScanSelect(result, projCnt, projNamesArray, attrDesc, op, attrValue_2, reclen);//////
}



const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
	int resultTupCnt = 0;

	// have a temporary record for output table
	char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

	// open "result" as an InsertFileScan object
	InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }	

	// open current table (to be scanned) as a HeapFileScan object
	HeapFileScan Scan(string(projNames->relName), status);
    if (status != OK) { return status; }

	// check if an unconditional scan is required
	if (attrDesc == NULL){
		status = Scan.startScan(0,
                            0,
                            STRING,
                            NULL,
                            EQ);
    	if (status != OK) { return status; }
	}
	else{
		status = Scan.startScan(attrDesc->attrOffset,
                                attrDesc->attrLen,
                                (Datatype) attrDesc->attrType,
                                filter,
                                op);
        if (status != OK) { return status; }
	}

	// scan the current table
	RID recRID;
    Record Rec;
	while (Scan.scanNext(recRID) == OK)
    {
        status = Scan.getRecord(Rec);
        ASSERT(status == OK);

        // we have a match, copy data into the output record
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            // if find a record, then copy stuff over to the temporary record (memcpy)
            memcpy(outputData + outputOffset, (char *)Rec.data + projNames[i].attrOffset,
                           projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        } 
        // insert into the output table
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        ASSERT(status == OK);
        resultTupCnt++;
    } // end scan
    // printf("tuple selection produced %d result tuples \n", resultTupCnt);
    status = Scan.endScan();
    return status;
}


