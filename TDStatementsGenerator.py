
import avro
import sys
import tempfile
import google
import urllib2
from google.protobuf import reflection as _reflection
from avro import schema
from google.protobuf import descriptor_pb2
from google.protobuf import text_format
from google.protobuf import descriptor
from google.protobuf.descriptor import FileDescriptor
import json


metadata_fields_map = {

     "ROW_ID" :  "ID as ROW_ID" , 
     "DV_FLAG" :  "'N'" ,
     "RPL_TRX_TYPE"   :  "metadata_idr_op_code",
     "RPL_TRX_TIME"  :  "metadata_idr_op_time" , 
     "RPL_CAPXINFO" : "RPL_CAPXINFO",
     "NAME_VALUE"   : "'single'",
     "CurrentlyProcessedFileName" : "metadata_reserved_1"
}

businessNameToCNameMap={}

def generateTagsSequenceForSingleFields(avro_text, proto_schema_fields, module_name, version):
    proto_schema_fields_names_to_numbers_map = {}
    value_tags_arr=[]
    proto_schema_fields_numbers_to_generated_objects={}
    id_field_number = 0
    for field in proto_schema_fields:
        proto_schema_fields_names_to_numbers_map[field.name] = field.number
        if (field.name == "id"):
            id_field_number = field.number
            
    avro_schema = schema.parse(avro_text)
    
    for field in avro_schema.fields:
        sc,parent=checkRecordSchema(field)
        
        
        if( sc != None and field.name != "metadata"):
                  continue;
        if(field.name == "metadata"):
            for subField in sc.fields:
               addObjsForSingleField("metadata_",subField,  proto_schema_fields_names_to_numbers_map, value_tags_arr, proto_schema_fields_numbers_to_generated_objects)
        else:
            addObjsForSingleField("",field,  proto_schema_fields_names_to_numbers_map, value_tags_arr, proto_schema_fields_numbers_to_generated_objects)
                 
    
    value_tags_arr.sort()
    tags=""
    names=""
    tags_150=""
    tags_2=""
    for v in value_tags_arr:
        objs=proto_schema_fields_numbers_to_generated_objects.get(v)
        tags=tags+str(objs[1])+","
        names=names+objs[0]+","
        tags_150=tags_150+str(objs[2])+","
        tags_2=tags_2+str(objs[3])+","
        
       
    names = names.rstrip(",")   
    name="{0}_{1}_tagsSequenceForSingleFields.txt".format(module_name, version)
    file = open(name,"w")
    file.write(tags+"\n")
    file.write(names+"\n")
    file.write(tags_150+"\n")
    file.write(tags_2+"\n")
    file.close()  
        


def sortMultiFields(m,s,sg,v,m_name,s_name,sg_name,v_name):
    return str(m)+","+str(s)+","+str(sg)+","+str(v)+"," , m_name+","+s_name+","+sg_name+","+v_name , m_name+" varchar(300) ,"+s_name+" varchar(300), "+sg_name+" varchar(300),"+v_name+" varchar(300)"
def generateTagsSequence(avro_text, proto_schema_fields, module_name, version):
    proto_schema_fields_names_to_numbers_map = {}
    map_tag_value_to_its_tags={}
    value_tags_arr=[]
    id_field_number = 0
    for field in proto_schema_fields:
        proto_schema_fields_names_to_numbers_map[field.name] = field.number
        if (field.name == "id"):
            id_field_number = field.number
    
    avro_schema = schema.parse(avro_text)
    for field in avro_schema.fields:
        sc,parent=checkRecordSchema(field)
        if( sc != None and field.name != "metadata"):
            m,s,sg,v = 0,0,0,0
            m_name,s_name,sg_name,v_name="","","",""
            for structField in sc.fields:
               if structField.name == "VALUE": 
                   v_name="c_"+field.name
                   v=proto_schema_fields_names_to_numbers_map.get(field.name)
                   value_tags_arr.append(v)
               elif structField.name == "m":    
                   m_name= field.name + "_" + structField.name               
                   m=proto_schema_fields_names_to_numbers_map.get(m_name)
               elif structField.name == "sg":
                   sg_name=field.name + "_" + structField.name 
                   sg=proto_schema_fields_names_to_numbers_map.get(sg_name)
               elif structField.name == "s":
                   s_name=field.name + "_" + structField.name 
                   s=proto_schema_fields_names_to_numbers_map.get(s_name)
            
        
            tags, names, names_with_types = sortMultiFields(m, s, sg, v, m_name, s_name, sg_name, v_name)
            objs=[]
            objs.append(tags)
            objs.append(names)
            objs.append(names_with_types)
            map_tag_value_to_its_tags[v]=objs              
    
    
    
    value_tags_arr.sort()
    tags_result=""
    names_result=""
    for v in value_tags_arr:
        objs=map_tag_value_to_its_tags.get(v)
        tags_result=tags_result+objs[0]
        names_result=names_result+objs[1]+","
    
    names_result = names_result.rstrip(",")   
    name="{0}_{1}_tagsSequenceForMultiFields.txt".format(module_name, version)
    file = open(name,"w")
    file.write(tags_result+"\n")
    file.write(names_result+"\n")
    file.close() 

def generateMultiFieldsCaseStatements(avro_text, proto_schema_fields, module_name, version):

    proto_schema_fields_names_to_numbers_map = {}
    id_field_number = 0
    for field in proto_schema_fields:
        proto_schema_fields_names_to_numbers_map[field.name] = field.number
        if (field.name == "id"):
            id_field_number = field.number

    avro_schema = schema.parse(avro_text)
    multi_fields_tags_to_names_map={}
    for field in avro_schema.fields:
        
        sc, parent = checkRecordSchema(field)
        if (sc != None and field.name != "metadata"):
            #print("multiField")
            #print(field)
            for structField in sc.fields:
                if structField.name == "VALUE":
                    v_name=field.name
                    #print("struct field name" + v_name)
                    fieldNum = proto_schema_fields_names_to_numbers_map.get(field.name)
                    v = fieldNum
                    multi_fields_tags_to_names_map[v] = v_name
            #print("---------------------------------------------------------")

    output="case VALUE_TAG \n"
    for key in multi_fields_tags_to_names_map:
        name=multi_fields_tags_to_names_map.get(key)
        cName=businessNameToCNameMap.get(name)
        output=output+"when '"+str(key)+"'  then '"+cName+"'\n"
    output=output+"END"
    print(output)
    name="{0}_{1}_caseStatements.txt".format(module_name, version)
  
    #file = open(name,"w")
    #file.write(output)
    #file.close() 
    return output    
                      

def addObjsForSingleField(prefix, field, proto_schema_fields_names_to_numbers_map, single_fields_nums_arr, proto_schema_fields_numbers_to_generated_objects):
    name=prefix+field.name
    fieldNum=proto_schema_fields_names_to_numbers_map.get(name)
    single_fields_nums_arr.append(fieldNum)
    objs=[]
    objs.append(name)
    objs.append(fieldNum)
    objs.append(150)
    objs.append(2)
    type=getType2(field.type)
    objs.append(type)
    proto_schema_fields_numbers_to_generated_objects[fieldNum]=objs

def createSingleFieldsStatements(single_fields_nums_arr, proto_schema_fields_numbers_to_generated_objects, version , module_name):

    single_fields_nums_arr.sort()
    single_fields_names="offset , "
    single_fields_nums=""
    single_fields_and_types="offset varchar(200), "
    for num in single_fields_nums_arr:
        objs=proto_schema_fields_numbers_to_generated_objects.get(num)
        single_fields_names=single_fields_names+objs[0]+","
        single_fields_nums=single_fields_nums+objs[1]+";"
        single_fields_and_types=single_fields_and_types+objs[0]+" "+objs[2]+" ,";

    single_fields_names = single_fields_names.rstrip(",")
    single_fields_and_types = single_fields_and_types.rstrip(',')
    single_table_name="rtpoc_{0}_{1}_single".format(version, module_name)
    single_view_name="rtpoc_{0}_{1}_single_vw".format(version , module_name)
    single_create_table_statement="create multiset table {0} ( {1} ); ".format(single_table_name, single_fields_and_types)
    f.write("drop table " + single_table_name+";\n")
    f.write(single_create_table_statement+"\n")
    single_view_table_statement="replace view {0} \n as select {1} from {2} ( __fnc__( '{3}' , rtpoc_{4}_land.message_bytes, rtpoc_{4}_land.partition_offset) returns ({5} )) as q1;".format(single_view_name,single_fields_names,"table",single_fields_nums , module_name,single_fields_and_types)
    f.write(single_view_table_statement+"\n")
    single_procedure_name="rtpoc_{0}_{1}_single_ps".format(version, module_name)
    single_sp_creation_statement = "create procedure {0} () \n begin \n insert into {1} ( {2} ) select {3} from {4} ; \n end;".format(
        single_procedure_name , single_table_name, single_fields_names, single_fields_names, single_view_name)

    f.write(single_sp_creation_statement+"\n")
    f.close()
    single_sp_insert_select_statement="insert into {0} ( {1} ) select {2} from {3};\n".format(single_table_name,single_fields_names,single_fields_names,single_view_name)
    sp_insert_select_statements = sp_insert_select_statements + single_sp_insert_select_statement
    procedure_create_statement = "create procedure {0} () \n begin \n {1} \n end;".format(procedure_name, sp_insert_select_statements)

def createMultiFieldsStatements(multi_fields_tags_arr, proto_schema_fields_numbers_to_generated_objects, version, module_name, case_statement):
    multi_fields_tags_arr.sort()
    table_fields_names="RPL_CAPXINFO,NAME_VALUE,M,S,SG,C,SEQUENCE"
    table_fields_names_with_types="RPL_CAPXINFO VARCHAR(600) CHARACTER SET LATIN NOT CASESPECIFIC,NAME_VALUE VARCHAR(600),M VARCHAR(600),S VARCHAR(600),SG VARCHAR(600),C VARCHAR(6000),SEQUENCE VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC"
    
    tags_result=""
    names_result=""
    for v in multi_fields_tags_arr:
        objs=proto_schema_fields_numbers_to_generated_objects.get(v)
        tags_result=tags_result+objs[0]
        names_result=names_result+objs[1]+","
    
    tags_result  = tags_result.rstrip(",")
    names_result = names_result.rstrip(",")   
    
    #table
    #table_name="rtpoc_{0}_{1}_multi_value_staging".format(version,module_name) 
    table_name="{0}_multi".format(module_name)
    drop_table_statement="drop table {0};".format(table_name)
    create_table_statement="CREATE SET TABLE {0} ,FALLBACK,\n NO BEFORE JOURNAL,\n NO AFTER JOURNAL,\n CHECKSUM = DEFAULT, \n DEFAULT MERGEBLOCKRATIO \n ( \n {1} \n ) \n PRIMARY INDEX ( SEQUENCE );".format(table_name , table_fields_names_with_types)
    
    #view 
    count=len(tags_result.split(","))
    land_table_name="rtpoc_{0}_land".format(module_name)
    view_name="rtpoc_{0}_{1}_multi_value_staging_vw".format(version,module_name)
    create_view_statement="replace view RTT24STG.{0} \n as \n select STRTOK(id,'_',5) as RPL_CAPXINFO ,STRTOK(RPL_CAPXINFO,'_',2) as SEQUENCE ,f as VALUE_TAG, i as ROW_INDEX , coalesce(max(m),1) as M , coalesce(max(s),1) as S , coalesce(max(sg),1) as SG , max(v) as V \n FROM RTT24STG.to_t24_multi \n ( \n on (select  partition_offset, message_bytes, message_blob, message_size from RTT24DV.{1} ) \n using \n tagCount({2}) \n valLength({3}) \n tagList('{4}') \n ) AS D \n group by RPL_CAPXINFO,VALUE_TAG,ROW_INDEX ;".format(view_name,land_table_name,count,600,tags_result)
    
    #procedure
    
    procedure_name = "rtpoc_{0}_{1}_prerequisite_ps".format(version, module_name)
    procedure_select_statement_fields="select RPL_CAPXINFO , {0} as NAME_VALUE , m as M , s as S , sg as SG , v as C , SEQUENCE from RTT24STG.{1} ".format(case_statement, view_name)
    create_procedure_statement="replace procedure RTT24PRC.{0} () \n begin \n insert into RTT24DV.{1} ({2}) \n {3}; \n end ;".format(procedure_name, table_name , table_fields_names ,procedure_select_statement_fields)
    
    clear_statement="delete from {0};".format(table_name);
    statements=[]
    statements.append(drop_table_statement)
    statements.append(create_table_statement)
    statements.append(create_view_statement)
    statements.append(create_procedure_statement)
    
    
    return statements,names_result,clear_statement
    

def createSingleFieldsStatementsV2(single_fields_nums_arr, proto_schema_fields_numbers_to_generated_objects, version , module_name):
    single_fields_nums_arr.sort()
    count=len(single_fields_nums_arr)
    table_fields_names=""
    table_fields_names_with_types=""
    procedure_select_statement_fields=""
    single_fields_nums=""
    
    for key in metadata_fields_map:
       table_fields_names=table_fields_names+key+","
       type="varchar(200)"
       if key == "DV_FLAG":
           type="varchar(10)"
          
       table_fields_names_with_types=table_fields_names_with_types+key+" " + type+","
       procedure_select_statement_fields=procedure_select_statement_fields+metadata_fields_map.get(key)+","
    
    view_fields_names="STRTOK(id,'_',5)  as RPL_CAPXINFO , "
    single_fields_varLength=""
    single_fields_uniCodeFlag=""    
    
    for num in single_fields_nums_arr:
       objs=proto_schema_fields_numbers_to_generated_objects.get(num)
       name=objs[0]
       tag_number=objs[1]    
       varLength=objs[2]
       unicode_type=objs[3]
       type=objs[4]
       
       if not name.startswith('metadata_'):
          name=businessNameToCNameMap.get(name)
          table_fields_names=table_fields_names+name+","
          table_fields_names_with_types=table_fields_names_with_types+name+" "+type+" ,"
          procedure_select_statement_fields=procedure_select_statement_fields+name+","
          
       view_fields_names=view_fields_names+"t"+str(tag_number)+" as "+name+","
       single_fields_nums=single_fields_nums+str(tag_number)+","
       single_fields_varLength=single_fields_varLength+str(varLength)+","
       single_fields_uniCodeFlag=single_fields_uniCodeFlag+str(unicode_type)+","
    
    
    
    table_fields_names=table_fields_names.rstrip(",")
    table_fields_names_with_types=table_fields_names_with_types.rstrip(",")
    procedure_select_statement_fields=procedure_select_statement_fields.rstrip(",")
    view_fields_names=view_fields_names.rstrip(",")
    single_fields_nums=single_fields_nums.rstrip(",")
    single_fields_varLength=single_fields_varLength.rstrip(",")
    single_fields_uniCodeFlag=single_fields_uniCodeFlag.rstrip(",")
    
    #single_table_name="rtpoc_{0}_{1}_single".format(version, module_name)
    single_table_name="{0}_single".format(module_name)
    single_view_name="rtpoc_{0}_{1}_single_vw".format(version , module_name)
    single_create_table_statement="create multiset table {0} ( {1} ); ".format(single_table_name, table_fields_names_with_types)
    drop_statement = "drop table " + single_table_name+";"
    single_view_table_statement="replace view RTT24STG.{0} \n  as select {1}  from RTT24STG.{2} ( on ( select partition_offset, message_bytes, message_blob from RTT24DV.rtpoc_{3}_land ) \n using \n tagCount({4}) \n tagList('{5}') \n valLengthList('{6}') \n valCharsetList('{7}') \n ) AS D ;".format(single_view_name,view_fields_names,"stgt24_t.to_t24_single" , module_name , count, single_fields_nums, single_fields_varLength,single_fields_uniCodeFlag)
    single_procedure_name="rtpoc_{0}_{1}_single_ps".format(version, module_name)
    single_sp_creation_statement = "replace procedure RTT24PRC.{0} () \n begin \n insert into RTT24DV.{1} ( {2} ) \n select  \n {3} from RTT24STG.{4} ; \n end;".format(
        single_procedure_name , single_table_name, table_fields_names, procedure_select_statement_fields, single_view_name)   

    clear_statement="delete from {0};".format(single_table_name); 
    statements=[]
    statements.append(drop_statement)
    statements.append(single_create_table_statement)
    statements.append(single_view_table_statement)
    statements.append(single_sp_creation_statement)
    
    return statements,clear_statement
    
    
    

def generateFieldsWithTags(avro_text, proto_schema_fields, module_name, version):
    
    proto_schema_fields_names_to_numbers_map = {}
    proto_schema_fields_numbers_to_generated_objects={}
    for field in proto_schema_fields:  
        proto_schema_fields_names_to_numbers_map[field.name] = field.number
        

    avro_schema = schema.parse(avro_text)
    single_fields_nums_arr=[] # array of single value fields's tags numbers 
    multi_value_tags_arr=[] # array of multi value fields's tags numbers
    
    f=open("{0}_{1}_statements.txt".format(module_name,version), "w")
            
    for field in avro_schema.fields:
        sc,parent=checkRecordSchema(field)
        if( sc != None and field.name != "metadata" and field.name != "RPL"):
            m,s,sg,v = 0,0,0,0
            m_name,s_name,sg_name,v_name="","","",""
            for structField in sc.fields:
               if structField.name == "VALUE": 
                   v_name="c_"+field.name
                   v_name=v_name+"("+businessNameToCNameMap.get(field.name)+")"
                   v=proto_schema_fields_names_to_numbers_map.get(field.name)
                   multi_value_tags_arr.append(v)
               elif structField.name == "m":    
                   m_name= field.name + "_" + structField.name               
                   m=proto_schema_fields_names_to_numbers_map.get(m_name)
               elif structField.name == "sg":
                   sg_name=field.name + "_" + structField.name 
                   sg=proto_schema_fields_names_to_numbers_map.get(sg_name)
               elif structField.name == "s":
                   s_name=field.name + "_" + structField.name 
                   s=proto_schema_fields_names_to_numbers_map.get(s_name)
            
        
            tags, names, names_with_types = sortMultiFields(m, s, sg, v, m_name, s_name, sg_name, v_name)
            objs=[]
            objs.append(tags)
            objs.append(names)
            objs.append(names_with_types)
            proto_schema_fields_numbers_to_generated_objects[v]=objs
        else:
            if(field.name == "metadata" or field.name == "RPL"):
                prefix=field.name+"_"
                for subField in sc.fields:
                    addObjsForSingleField(prefix,subField,  proto_schema_fields_names_to_numbers_map, single_fields_nums_arr, proto_schema_fields_numbers_to_generated_objects)
            else:
                 addObjsForSingleField("",field,  proto_schema_fields_names_to_numbers_map, single_fields_nums_arr, proto_schema_fields_numbers_to_generated_objects)        
    
    
    # create multi_value fields statements
    
    caseStatement=generateMultiFieldsCaseStatements(avro_text, proto_schema_fields, module_name, version)
    multi_Fields_statements,multi_fields_names_corresponding_to_tags,multi_clear_statement=createMultiFieldsStatements(multi_value_tags_arr, proto_schema_fields_numbers_to_generated_objects, version , module_name,caseStatement)
    
    # create single value fields  statements
    single_statements,single_clear_statement = createSingleFieldsStatementsV2(single_fields_nums_arr, proto_schema_fields_numbers_to_generated_objects, version , module_name)  

    clear_procedure_name="rtpoc_{0}_{1}_clear_staging".format(version, module_name)
    run_id_name="{0}_RUN_ID".format(module_name)
    clear_run_id_statement ="delete from {0};".format(run_id_name)
    clear_procedure_statement="replace procedure {0}() \n begin \n {1} \n {2} \n {3} \n end;".format(clear_procedure_name,multi_clear_statement,single_clear_statement, clear_run_id_statement)
    f.write("--multi value fields staements \n")
    f.write("--=========================================\n\n")
    
    for m_statement in multi_Fields_statements:
       f.write(m_statement+"\n")
       f.write("--=================================\n")
    
    f.write("--single value fields statements \n")
    f.write("--=====================================\n\n")
    
    for s_statement in single_statements:
       f.write(s_statement+"\n")
       f.write("--=================================\n")

    f.write(clear_procedure_statement)  
    f.close()    
    
    #print "--================================================================================================================================ \n"
    #print procedure_create_statement

def generateSchemaHiveStatements(avro_text):
    avro_schema=schema.parse(avro_text)

    single_fields_for_select=""
    single_fields_for_create=""

    for field in avro_schema.fields:
        sc,parent = checkRecordSchema(field)
        if(sc != None):
            fields_for_create=""
            fields_for_select=""
            for structField in sc.fields:
                name=field.name+"_"+structField.name
                if fields_for_select != "":
                    fields_for_select = fields_for_select + "," + name
                else:
                    fields_for_select = name

                fields_for_create=fields_for_create+name+" string,\n"


            fields_for_create=fields_for_create.rstrip(",\n")
            fields_for_select=fields_for_select.rstrip(",")
            create_statement="create table {0} ( {1} )".format(field.name, fields_for_create)
            select_statement="select {0} from {1} ".format(fields_for_select,field.name)
            print("statements for "+field.name)
            print(create_statement)
            print(select_statement)
            print("=============================================================================================================")
        else:

            single_fields_for_select= single_fields_for_select+field.name+","
            type=getType2(field.type)
            if "array" in type:
                a=type.split(' ')
                type="array<{0}>".format(a[0])
            single_fields_for_create=single_fields_for_create+field.name+" "+type+",\n"

    print("single statements ")
    print "create table account ( {0} ) ".format(single_fields_for_create)
    print "select {0} from account ".format(single_fields_for_select)









def checkRecordSchema(field):
    
    type = field.type
    sc = None
    parent = None
    
    if isinstance(type, schema.UnionSchema):
       
        type = type._schemas[1]
        if isinstance(type, schema.ArraySchema) and isinstance(type.items, schema.RecordSchema):
            sc = type.items
            parent = "array"
        elif isinstance(type, schema.RecordSchema):
            sc = type
            parent = "union"
    elif isinstance(type, schema.ArraySchema) and isinstance(type.items, schema.RecordSchema):
        sc = type.items
        parent = "array"
    elif isinstance(type, schema.RecordSchema):
        sc = type
        parent = None
    
    

    return sc, parent

def  constructBusinessNameToCNameMap(cNameToBusinessNameMap):
    
    for key in cNameToBusinessNameMap:
       value=cNameToBusinessNameMap.get(key)
       print("key " + key + " value "+ value)
       businessNameToCNameMap[value]=key
       
    
def  getType2(typ):

    if isinstance(typ,schema.UnionSchema):
            return getType2(typ._schemas[1])

    if(isinstance(typ,schema.ArraySchema)):

            return  getType2(typ.items)+" array"

    #if(isinstance(typ,schema.RecordSchema)):
      #      print("inside getType 2 ", typ, "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&*****************")
           #return getORAddStruct(messages,typ)
    if(isinstance(typ,schema.MapSchema)):
            #valType= getType2(typ.values, messages)
            return "map < varchar(200), varchar(200) > "
    else:

            val =typ.fullname.replace("\"","",-1)
            if val == "int":
                return "varchar(200)"
            elif  val == "long":
                return  "varchar(200)"
            elif val  == "double":
                return "varchar(200)"
            elif val  == "string":
                return "varchar(200)"
            return val


if __name__ == "__main__":
    sc1=urllib2.urlopen(sys.argv[1]).read()
    protoText = urllib2.urlopen(sys.argv[2]).read()
    cNameToBusinessNameMap=urllib2.urlopen(sys.argv[3]).read()
    cNameToBusinessNameMap=json.loads(cNameToBusinessNameMap) 
    module_name=sys.argv[4]    
    version=sys.argv[5]
    constructBusinessNameToCNameMap(cNameToBusinessNameMap)
    file_descriptor_proto = descriptor_pb2.FileDescriptorProto()
    text_format.Merge(protoText, file_descriptor_proto)
    protoSchema = file_descriptor_proto
    proto_schema_fields = protoSchema.message_type[0].field
    generateFieldsWithTags(sc1, proto_schema_fields, module_name, version)
    #generateMultiFieldsCaseStatements(sc1,proto_schema_fields, module_name, version)
    #generateTagsSequence(sc1, proto_schema_fields, module_name, version)
    #generateTagsSequenceForSingleFields(sc1, proto_schema_fields, module_name, version)
    #generateSchemaHiveStatements(sc1)
