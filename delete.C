#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const   char *attrValue)
{	
	Status status;
	// open current table (to be scanned) as a HeapFileScan object
	HeapFileScan Scan(relation, status);
    if (status != OK) { return status; }
	
	// if (attr->attrType != type)
    // {
    //     return ATTRTYPEMISMATCH;
    // }

	// check if an unconditional scan is required
	if (attrName.empty()){
		status = Scan.startScan(0,
                            0,
                            STRING,
                            NULL,
                            EQ);
    	if (status != OK) { return status; }
	}
	else{
		AttrDesc *attr = new AttrDesc();
    	status = attrCat->getInfo(relation, attrName, *attr);
    	if (status != OK){ return status; }
		
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

		status = Scan.startScan(attr->attrOffset,
                                attr->attrLen,
                                (Datatype) attr->attrType,
                                attrValue_2,
                                op);
        if (status != OK) { return status; }
	}
	// cout<<"sfakdjlfajdf"<<endl;
	// scan the current table
	RID recRID;
    Record Rec;
	while (Scan.scanNext(recRID) == OK)
    {	
		cout << "delete" << endl;
		status = Scan.deleteRecord();
		if (status != OK) { return status; }
    } // end scan
	// cout<<"sfakdjlfajdf"<<endl;
	// part 6
	return OK;
}