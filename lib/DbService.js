/**
 * Created by yuanjianxin on 2018/4/30.
 */
const Sequelize=require('sequelize');
const RedisHandler=require('yue-redis-handler');
const _path=require('path');
global.TableMap=global.TableMap || new Map();
global.DBModel=global.DBModel || {};
global.__MODULE_PATH__=null;
global.__EXPIRE_TIME__=30;
const server=require('../grpc/server');
const generateDBModel=require('./generateDBModel');

module.exports=class DbService{


    static get instance(){
        if(!DbService._instance)
            DbService._instance=new DbService();
        return DbService._instance;
    }

    constructor(){
        this.mappingConf=null; // 项目-库-表 映射配置项，支持分库分表
        this.databaseConf=null;// 数据库链接配置项,一个数组，支持多个数据库链接配置
        this.redisConf=null; // redis 链接配置项
        this.port=null;// db service 端口配置项

        this.DBModel=null;

        this.modulePath=null;
    }

    /**
     * 设置映射配置
     * @param conf
     */
    setMappingConf(conf){
        this.mappingConf=conf;
    }

    /**
     * 设置数据库链接配置
     * @param conf
     */
    setDatabaseConf(conf){
        this.databaseConf=conf;
    }

    /**
     * 设置redis 配置
     * @param conf
     */
    setRedisConf(conf){
        this.redisConf=conf;
    }

    /**
     * 设置端口
     * @param port
     */
    setPort(port){
        this.port=port;
    }

    setExpireTime(expireTime){
        global.__EXPIRE_TIME__=expireTime
    }

    setModulePath(path){
        this.modulePath=path;
    }


    initDBModel(){
        if(this.databaseConf==null)
            throw new Error('==DatabaseConf should set first!==');
        this.DBModel=generateDBModel(this.databaseConf);
        DBModel=this.DBModel;
    }

    initRedis(){
        if(this.redisConf==null)
            throw new Error('==RedisConf should set first!==');
        RedisHandler.instance.init(this.redisConf);
    }

    get DbModel(){
        return this.DBModel;
    }


    generateTableMap(){
        if(this.mappingConf==null)
            throw new Error('==MappingConf should set first!==');
        let dbMap=this.mappingConf;
        Object.keys(dbMap).forEach(v=>{
            Object.keys(dbMap[v]).forEach(vv=>{
                dbMap[v][vv].forEach(t=>{
                    TableMap.set(v+'-'+t,vv);
                })
            })
        });
    }

    initModulePath(){
        if(this.modulePath==null)
            throw new Error('==ModulePath should set first!==');
        if(!_path.isAbsolute(this.modulePath))
            this.modulePath=_path.resolve(this.modulePath);

        if(!this.modulePath.endsWith('/'))
            this.modulePath=this.modulePath+'/';

        __MODULE_PATH__=this.modulePath;
    }

    get Sequelize(){
        return Sequelize;
    }

    async run(){
        this.initDBModel();
        this.initRedis();
        this.generateTableMap();
        this.initModulePath();

        if(this.port==null)
            throw new Error('==Port should set first!==');

        server(this.port).start((err,data)=>{
            console.error('====grpc err===', err);
            console.log('====data===', data);
        });
    }
};