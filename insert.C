#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	Status status;
	Record rec;
	RID outRid;
	unsigned int reclen = 0;

	// open current table (to be scanned) as a HeapFileScan object
	InsertFileScan resultRel(relation, status);
    if (status != OK) { return status; }	

	// get all attribute info in relation
	int relattrCnt;
	AttrDesc *attrDesc;
    status = attrCat->getRelInfo(relation, relattrCnt, attrDesc);
    if (status != OK){ return status; }

	// Make sure that attrCnt corresponds the relation attribute count
	if (attrCnt != relattrCnt) return OK;
	
	for (int i = 0; i < attrCnt; i ++){
		reclen += attrDesc[i].attrLen;
	}
	rec.data = (void *)malloc(reclen * attrCnt);
	rec.length = 0;

	// copy attrList to rec.data
	int dataOffset = 0;
	for (int i = 0; i < relattrCnt; i++){
		for (int j = 0; j < attrCnt; j++){
			if (attrList[j].attrValue == NULL)
				return BADCATPARM;	//TODO

			// find the same attribute, add to rec.data
			if (strcmp(attrDesc[i].attrName, attrList[j].attrName) == 0){
				
				int type = attrList[j].attrType;
				const char *attrValue_2;
				switch(type) {
				case INTEGER:{
					int new_val = atoi((char*)attrList[j].attrValue);
					attrValue_2 = (char *)&new_val;
					break;
				}
				case FLOAT:{
					float new_val = atof((char*)attrList[j].attrValue);
					attrValue_2 = (char *)&new_val;
					break;
				}
				case STRING:{
					attrValue_2 = (char*)attrList[j].attrValue;
					break;
				}
				default:
					break;
				}
				memcpy((char*)rec.data + attrDesc[i].attrOffset, attrValue_2, attrDesc[i].attrLen);
				rec.length += attrDesc[i].attrLen;
				break;
			}
		}
	}
	resultRel.insertRecord(rec, outRid);
	free(rec.data);
	return OK;
}