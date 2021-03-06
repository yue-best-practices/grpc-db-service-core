/**
 * Created by yuanjianxin on 2018/3/7.
 */
const RedisHandler=require('yue-redis-handler');
const fs=require('fs');
const _module_cache=new Map();
const AsyncAll=require('yue-asyncall');
function loadModule(_path) {
    if(!_module_cache.has(_path)){
        if(!fs.existsSync(_path))
            return null;
        _module_cache.set(_path,require(_path));
    }
    return _module_cache.get(_path);
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
        let key = database + '|' + table+"|"+paras;
        let res = await RedisHandler.instance.exec('get', key);
        if (res !== null){
            await RedisHandler.instance.exec("expire",key,__EXPIRE_TIME__);
            return JSON.parse(res);
        }

        let _path = __MODULE_PATH__+ database + '/' + table + '.js';


        let _module = loadModule(_path);

        if (!_module)
            return null;

        res = await _module.findById(paras)
        if (!res)
            return null;
        res = res.get({plain: true});
        RedisHandler.instance.exec('setex', key,__EXPIRE_TIME__,JSON.stringify(res));
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
        let key=database+'|'+table+"|"+where+"|"+paras;
        //查找主键id
        let id=await RedisHandler.instance.exec('get',key);
        if(id!==null){
            let res=await this.get(database,table,id);
            if(res){
                await RedisHandler.instance.exec("expire",key,__EXPIRE_TIME__);
                return res;
            }
            //todo 这里如果外键能找到存在redis中的主键id，但是数据库中或者redis中该记录没有了，那么就需要清除外键缓存
            if(!res)
                await RedisHandler.instance.exec('del',key);
        }


        //查DB
        let _path = __MODULE_PATH__ + database + '/' + table + '.js';

        let _module = loadModule(_path);
        if (!_module)
            return null;
        let condition={ where:{} };
        condition.where[where]=paras;
        let res=await _module.findOne(condition);
        if(!res)
            return null;
        res=res.get({ plain:true });
        await RedisHandler.instance.exec('setex',key,__EXPIRE_TIME__,res.id);
        await RedisHandler.instance.exec('setex',`${database}|${table}|${res.id}`,__EXPIRE_TIME__,JSON.stringify(res));
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
        let key = database + '|' + table+'|'+res.id;
        await RedisHandler.instance.exec('setex',key,__EXPIRE_TIME__,JSON.stringify(res));
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

        await _module.update(paras,where);

        // update to redis
        let res=await _module.findAll(where);

        res=res.map(v=>v.get({ plain:true }));

        let savePromiseList=[];

        res.forEach(v=>{
            savePromiseList.push((
                async (v)=>{
                    await RedisHandler.instance.exec("setex",`${database}|${table}|${v.id}`,__EXPIRE_TIME__,JSON.stringify(v));
                }
            )(v))
        });

        await AsyncAll(savePromiseList);

        return true;
    },

    /**
     * 删除 按主键删除
     * @param database
     * @param table
     * @param id
     * @returns {Promise.<boolean>}
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
        let delKeys=id.map(v=>`${database}|${table}|${v}`);
        await RedisHandler.instance.exec('del',...delKeys);

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
        id=id.map(v=>v.toString());

        // find from redis
        let key = database + '|' + table;
        let findKeys=id.map(v=>`${key}|${v}`);

        let res=await RedisHandler.instance.exec('mget',...findKeys) || [];
        res=res.map(v=>JSON.parse(v));
        let result=res.filter(v=>v);

        //这里需要更新 成功查出的数据 的过期时间
        let updateKeys=result.map(v=>`${key}|${v[field]}`);
        let updatePromise=[];
        updateKeys.forEach(v=>{
           updatePromise.push((
               async (v)=>{
                   await RedisHandler.instance.exec("expire",v,__EXPIRE_TIME__);
               }
           )(v))
        });

        await AsyncAll(updatePromise);

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

            // let redisParas=[];
            let savePromise=[];
            db_res.forEach(v=>{
               savePromise.push((
                   async (v)=>{
                       await RedisHandler.instance.exec("setex",`${key}|${v[field]}`,__EXPIRE_TIME__,JSON.stringify(v));
                   }
               )(v))
            });

            await AsyncAll(savePromise);

            // db_res.forEach(v=>{
            //     redisParas.push(v[field], JSON.stringify(v));
            // });
            //
            // redisParas.length && await RedisHandler.instance.exec('hmset',key,...redisParas);

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

        return await _module.count(where);
    },


    async sum(database,table,where,field){
        let _path = __MODULE_PATH__ + database + '/' + table + '.js';
        let _module = loadModule(_path);
        if (!_module)
            return null;

        return await _module.sum(field,where);
    },

    async query(dataSource,rawQuery,where){
        let sequelize=DBModel && DBModel[dataSource] || null;
        if(!sequelize)
            return null;
        let res=null;
        try{
            res=await sequelize.query(rawQuery,where);
        }catch (e){
            console.error('DbServiceCore Query Error!',e);
        }
        return res;
    }



};