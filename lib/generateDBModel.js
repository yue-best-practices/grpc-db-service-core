/**
 * Created by yuanjianxin on 2018/4/30.
 */
const Sequelize = require('sequelize');
const sequelize={};
const Op = Sequelize.Op;
const operatorsAliases = {
    $eq: Op.eq,
    $ne: Op.ne,
    $gte: Op.gte,
    $gt: Op.gt,
    $lte: Op.lte,
    $lt: Op.lt,
    $not: Op.not,
    $in: Op.in,
    $notIn: Op.notIn,
    $is: Op.is,
    $like: Op.like,
    $notLike: Op.notLike,
    $iLike: Op.iLike,
    $notILike: Op.notILike,
    $regexp: Op.regexp,
    $notRegexp: Op.notRegexp,
    $iRegexp: Op.iRegexp,
    $notIRegexp: Op.notIRegexp,
    $between: Op.between,
    $notBetween: Op.notBetween,
    $overlap: Op.overlap,
    $contains: Op.contains,
    $contained: Op.contained,
    $adjacent: Op.adjacent,
    $strictLeft: Op.strictLeft,
    $strictRight: Op.strictRight,
    $noExtendRight: Op.noExtendRight,
    $noExtendLeft: Op.noExtendLeft,
    $and: Op.and,
    $or: Op.or,
    $any: Op.any,
    $all: Op.all,
    $values: Op.values,
    $col: Op.col
};

module.exports=(databaseConf)=>{
    if(!(databaseConf instanceof Array))
        databaseConf=[databaseConf];
    databaseConf.forEach(v=>{
        v.operatorsAliases && (v.operatorsAliases=operatorsAliases);
        sequelize[v.name]=new Sequelize(v.database,v.username,v.password,v);
    });

    Object.values(sequelize).forEach(v=>v.authenticate().then(res=>console.log('db connect success!')).catch(err=>console.log('connect failed!',err)));
    return sequelize;
};