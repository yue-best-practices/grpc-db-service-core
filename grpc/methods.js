/**
 * Created by yuanjianxin on 2018/7/3.
 */
const messages=require('grpc-db-service-pb').DbService_pb;
const Method=require('../lib/Method');


const getDataBase=(dataSource,table)=>{
    let key = dataSource + '-' + table;
    return TableMap.get(key);
};


const get=async (call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let paras=JSON.parse(call.request.getParas());
    let database=getDataBase(dataSource,table);
    let res=await Method.get(database,table,paras);
    res=JSON.stringify(res);
    let response=new messages.getResponse();
    response.setResult(res);
    callback(null,response);
};

const getOne=async (call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where=JSON.parse(call.request.getWhere());
    let paras=JSON.parse(call.request.getParas());
    let database=getDataBase(dataSource,table);
    let res=await Method.getOne(database,table,where,paras);
    res=JSON.stringify(res);
    let response=new messages.getOneResponse();
    response.setResult(res);
    callback(null,response);
};

const list=async(call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where=JSON.parse(call.request.getWhere());
    let database=getDataBase(dataSource,table);
    let res=await Method.list(database,table,where);
    res=JSON.stringify(res);
    let response=new messages.listResponse();
    response.setResult(res);
    callback(null,response);
};

const save=async (call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where=JSON.parse(call.request.getWhere());
    let paras=JSON.parse(call.request.getParas());
    let database=getDataBase(dataSource,table);
    let res=await Method.save(database,table,where,paras);
    res=JSON.stringify(res);
    let response=new messages.saveResponse();
    response.setResult(res);
    callback(null,response);
};

const update=async(call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where=JSON.parse(call.request.getWhere());
    let paras=JSON.parse(call.request.getParas());
    let database=getDataBase(dataSource,table);
    let res=await Method.update(database,table,where,paras);
    let response=new messages.updateResponse();
    response.setResult(res);
    callback(null,response);
};

const del=async (call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let id=JSON.parse(call.request.getId());
    let database=getDataBase(dataSource,table);
    let res=await Method.del(database,table,id);
    let response=new messages.delResponse();
    response.setResult(res);
    callback(null,response);
};


const multiGet=async (call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let id=JSON.parse(call.request.getId());
    let database=getDataBase(dataSource,table);
    let res=await Method.multiGet(database,table,id);
    res=JSON.stringify(res);
    let response=new messages.multiGetResponse();
    response.setResult(res);
    callback(null,response);
};


const toOne=async (call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where = JSON.parse(call.request.getWhere());
    let paras=JSON.parse(call.request.getParas());
    let result=JSON.parse(call.request.getResult());
    let database=getDataBase(dataSource,table);
    let res=await Method.toOne(database,table,where,paras,result);
    res=JSON.stringify(res);
    let response=new messages.toOneResponse();
    response.setResult(res);
    callback(null,response);
};

const toMany=async(call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where = JSON.parse(call.request.getWhere());
    let paras=JSON.parse(call.request.getParas());
    let result=JSON.parse(call.request.getResult());
    let database=getDataBase(dataSource,table);
    let res=await Method.toMany(database,table,where,paras,result);
    res=JSON.stringify(res);
    let response=new messages.toManyResponse();
    response.setResult(res);
    callback(null,response);
};

const count=async(call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where = JSON.parse(call.request.getWhere());
    let database=getDataBase(dataSource,table);
    let res=await Method.count(database,table,where);
    res=JSON.stringify(res);
    let response=new messages.countResponse();
    response.setResult(res);
    callback(null,response);
};

const sum=async(call,callback)=>{
    let dataSource=JSON.parse(call.request.getDatasource());
    let table=JSON.parse(call.request.getTable());
    let where = JSON.parse(call.request.getWhere());
    let field = JSON.parse(call.request.getField());
    let database=getDataBase(dataSource,table);
    let res=await Method.sum(database,table,where,field);
    res=JSON.stringify(res);
    let response=new messages.sumResponse();
    response.setResult(res);
    callback(null,response);
};

module.exports={
    get,getOne,list,save,update,toOne,toMany,multiGet,count,sum,del
};
