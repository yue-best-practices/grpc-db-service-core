/**
 * Created by yuanjianxin on 2018/7/3.
 */
const grpc=require('grpc');
const services=require('grpc-db-service-pb').DbService_grpc_pb;
const server=new grpc.Server();
const methods=require('./methods');
server.addService(
    services.DbServiceService,
    methods
);

module.exports=(port)=>{
    server.bind(`0.0.0.0:${port}`,grpc.ServerCredentials.createInsecure());
    return server;
};