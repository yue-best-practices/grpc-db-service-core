/**
 * Created by yuanjianxin on 2018/4/30.
 */
const Sequelize = require('sequelize');
const sequelize={};
module.exports=(databaseConf)=>{
    if(!(databaseConf instanceof Array))
        databaseConf=[databaseConf];
    databaseConf.forEach(v=>{
        sequelize[v.name]=new Sequelize(v.database,v.username,v.password,v);
    });

    Object.values(sequelize).forEach(v=>v.authenticate().then(res=>console.log('db connect success!')).catch(err=>console.log('connect failed!',err)));
    return sequelize;
};