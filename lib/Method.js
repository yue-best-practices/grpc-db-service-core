/**
 * Created by yuanjianxin on 2018/3/7.
 */
const Sequelize =require('sequelize');
const RedisHandler=require('yue-redis-handler');
const fs=require('fs');
const _module_cache=new Map();
const Op=Sequelize.Op;
const AsyncAll=require('yue-asyncall');

const AllowdOperators=['gt','gte','lt','lte','ne','eq','not','between','notBetween','in','notIn','like','notLike','iLike','notIlike'];

function loadModule(_path) {
    if(!_module_cache.has(_path)){
        if(!fs.existsSync(_path))
            return null;
        _module_cache.set(_path,require(_path));
    }
    return _module_cache.get(_path);
}

function formatCondition(where) {
    let condition=where;
    let _where=where.where || {};
    condition.where={};
    Object.keys(_where).forEach(v=>{
        typeof _where[v] === 'object' ? Object.keys(_where[v]).forEach(vv=>{
                if (AllowdOperators.includes(vv)) {
                    condition.where[v] = condition.where[v] || {};
                    condition.where[v] = Object.assign({}, condition.where[v], {[Op[vv]]: _where[v][vv]});
                }
            }) : (condition.where[v] = _where[v]);
    })
    return condition;
}

module.exports={

    /**
     * 从表中按主键ID 查
     * @param database
     * @param table
     * @param paras
     * @returns {Promise.<*>}
     */
    async get(database,table,paras){
        //先查redis 中是否有
        let key = database + '|' + table;
        let res = await RedisHandler.instance.exec('hget', key, paras);
        if (res !== null)
            return JSON.parse(res);

        let _path = __MODULE_PATH__+ database + '/' + table + '.js';


        let _module = loadModule(_path);

        if (!_module)
            return null;

        res = await _module.findById(paras)
        if (!res)
            return null;
        res = res.get({plain: true});
        RedisHandler.instance.exec('hset', key, paras, JSON.stringify(res));
        return res;
    },

    /**
     * 从表中按外键 查 一个
     * @param database
     * @param table
     * @param where
     * @param paras
     * @returns {Promise.<*>}
     */
    async getOne(database,table,where,paras){
        let key=database+'|'+table;
        let field=where+'|'+paras;
        //查找主键id
        let id=await RedisHandler.instance.exec('hget',key,field);
        if(id!==null)
            return await this.get(database,table,id);

        //查DB
        let _path = __MODULE_PATH__ + database + '/' + table + '.js';

        let _module = loadModule(_path);
        if (!_module)
            return null;
        let condition={ where:{} }
        condition.where[where]=paras;
        let res=await _module.findOne(condition);
        if(!res)
            return null;
        res=res.get({ plain:true });
        await RedisHandler.instance.exec('hset',key,field,res.id);
        await RedisHandler.instance.exec('hset',key,res.id,JSON.stringify(res));
        return res;
    },

    /**
     * 根据条件查询数据
     * @param database
     * @param table
     * @param where
     * @returns {Promise.<*>}
     */
    async list(database,table,where){
        //查DB
        let _path = __MODULE_PATH__ + database + '/' + table + '.js';

        let _module = loadModule(_path);
        if (!_module)
            return null;

        if(typeof where === 'string')
            where=JSON.parse(where);
        where=formatCondition(where);
        let res=await _module.findAll(where);
        res=res.map(v=>v.get({ plain:true }));
        return res;
    },


    /**
     * 保存/ 添加/ 修改
     * @param database
     * @param table
     * @param where
     * @param paras
     * @returns {Promise.<*>}
     */
    async save(database,table,where,paras){
        let _path = __MODULE_PATH__ + database + '/' + table + '.js';

        let _module = loadModule(_path);
        if (!_module)
            return null;

        if(typeof paras === 'string')
            paras=JSON.parse(paras);
        //先存db
        let id=where && paras[where] || null;
        let res=null;
        if(id){
            // update
            res = await _module.findById(id);
            if (!res) {
                res = await _module.create(paras);
            } else {
                res = res.get({plain: true});
                paras = Object.assign({}, res, paras);
                await _module.update(paras, {where: {id}});
                res = await _module.findById(id);
            }
        }else{
            // add
            res=await _module.create(paras);
        }
        res=res.get({ plain:true });
        // 再存redis
        let key = database + '|' + table;
        await RedisHandler.instance.exec('hset',key,res.id,JSON.stringify(res));
        return res;
    },

    /**
     * 按条件修改
     * @param database
     * @param table
     * @param where
     * @param paras
     */
    async update(database,table,where,paras){

        let _path = __MODULE_PATH__ + database + '/' + table + '.js';

        let _module = loadModule(_path);
        if (!_module)
            return false;

        // update to DB
        if(typeof where === 'string')
            where=JSON.parse(where);

        if(typeof paras === 'string')
            paras=JSON.parse(paras);

        where=formatCondition(where);

        await _module.update(paras,where);

        // update to redis
        let res=await _module.findAll(where);

        res=res.map(v=>v.get({ plain:true }));

        let redisParas=[];

        res.forEach(v=>{
            redisParas.push(v.id,JSON.stringify(v));
        });

        let key = database + '|' + table;

        await RedisHandler.instance.exec('hmset',key,...redisParas);

        return true;
    },

    /**
     * 删除 按主键删除
     * @param database
     * @param table
     * @param id
     * @returns {Promise.<void>}
     */
    async del(database,table,id){
        !(id instanceof Array) && (id=[id]);

        // delete from db
        let _path =__MODULE_PATH__+ database + '/' + table + '.js';

        let _module = loadModule(_path);
        if (!_module)
            return false;
        await _module.destroy({ where:{ id }});

        // delete from redis
        let key = database + '|' + table;
        await RedisHandler.instance.exec('hdel',key,...id);

        return true;
    },

    /**
     * 根据 id 批量查询
     * @param database
     * @param table
     * @param id
     * @param field
     * @returns {Promise.<Array>}
     */
    async multiGet(database,table,id,field="id"){
        !(id instanceof Array) && (id=[id]);
        id=[...new Set(id)];
        id=id.map(v=>id.toString());

        // find from redis
        let key = database + '|' + table;

        let res=await RedisHandler.instance.exec('hmget',key,...id) || [];
        res=res.map(v=>JSON.parse(v));
        let result=res.filter(v=>v);

        //如果redis查出的数量少，则从DB 中补充查询
        if(result.length<id.length){

            let resultIdList=result.map(v=>v[field]);
            let _path = __MODULE_PATH__ + database + '/' + table + '.js';

            let _module = loadModule(_path);
            if (!_module)
                return result;

            let condition={where:{}};
            condition.where[field]=id.filter(v=>!resultIdList.includes(v));
            let db_res=await _module.findAll(condition) || [];
            db_res=db_res.map(v=>v.get({ plain:true }));

            let redisParas=[];

            db_res.forEach(v=>{
                redisParas.push(v[field], JSON.stringify(v));
            });

            redisParas.length && await RedisHandler.instance.exec('hmset',key,...redisParas);

            result=[...result,...db_res];
        }

        return result;

    },

    /**
     * toOne
     * @param database
     * @param table
     * @param where
     * @param paras
     * @param result
     * @returns {Promise.<Array>}
     */
    async toOne(database,table,where,paras,result=[]){
        let fields=where.split(':');
        let from_field=fields[0];
        let to_field=fields[1];

        let promiseList=[];

        result.forEach(v=>{
            promiseList.push((
                async (v)=>{
                    let id=v[from_field] || null;
                    if(id!==null){
                        if(to_field === 'id'){
                            v[paras]=await this.get(database,table,id);
                        }else{
                            v[paras]=await this.getOne(database,table,to_field,id);
                        }
                    }
                }
            )(v));
        });

        await AsyncAll(promiseList);

        return result;
    },

    /**
     * toMany
     * @param database
     * @param table
     * @param where
     * @param paras
     * @param result
     * @returns {Promise.<Array>}
     */
    async toMany(database,table,where,paras,result=[]){
        let _path = __MODULE_PATH__+ database + '/' + table + '.js';
        let _module = loadModule(_path);
        if (!_module)
            return result;

        let fields=where.split(':');
        let from_field=fields[0];
        let to_field=fields[1];
        let promiseList=[];

        result.forEach(v=>{
            promiseList.push((
                async (v)=>{
                    let id=v[from_field] || null;
                    if(id!==null){
                        let condition={ where:{} };
                        condition.where[to_field]=id;
                        let res=await _module.findAll(condition);
                        res=res.map(v=>v.get({plain:true}));
                        v[paras]=res;
                    }
                }
            )(v));
        })

        await AsyncAll(promiseList);

        return result;
    },


    async count(database,table,where){
        let _path = __MODULE_PATH__+ database + '/' + table + '.js';
        let _module = loadModule(_path);
        if (!_module)
            return null;

        where=formatCondition(where);

        return await _module.count(where);
    },


    async sum(database,table,where,field){
        let _path = __MODULE_PATH__ + database + '/' + table + '.js';
        let _module = loadModule(_path);
        if (!_module)
            return null;

        where=formatCondition(where);

        return await _module.sum(field,where);
    }



};