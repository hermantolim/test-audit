db = db.getSiblingDB("rootdb");
db.createUser({
    user: "root",
    pwd: "root",
    roles: [{
        role: 'readWrite',
        db: "rootdb"
    }]
});
