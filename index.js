const fs = require('fs')
const path = require('path')

class Database{
    createcluster(databaseName){
        if(fs.existsSync(path.join(__dirname,databaseName))){
            throw new Error(`database ${databaseName} already exists`)
        }else{
            fs.mkdirSync(path.join(__dirname,databaseName))
        }        
    }
    createSchema(databaseName,schemaName,tablename,columns){
        const schemapath = path.join(databaseName,`%${schemaName}#schema.json`)
        const tablepath = path.join(databaseName,`${tablename}.json`) 

        if (fs.existsSync(schemapath)){
            throw new Error(`schema ${schemaName} already exists`)
        }

        const schemacolumn = columns.reduce((acc,column)=>{
            acc[column]=null
            return acc
        },{})
        
        const data = {schemacolumn}
        const tabledata = []
        fs.writeFileSync(schemapath,JSON.stringify(data))
        fs.writeFileSync(tablepath,JSON.stringify(tabledata))
    }

    insert(databaseName,schemaName,tablename,rowdata){
        const schemapath = path.join(databaseName,`%${schemaName}#schema.json`)
        const tablepath = path.join(databaseName,`${tablename}.json`)

        if (!fs.existsSync(databaseName) || !fs.existsSync(schemapath)||!fs.existsSync(tablepath)){
            throw new Error("request does not exists")
        }

        const schemadata = JSON.parse(fs.readFileSync(schemapath))
        const tabledata = JSON.parse(fs.readFileSync(tablepath))

        const newrow =  Object.keys(schemadata.schemacolumn).reduce((acc,column)=>{
            if (column in rowdata) {
                acc[column] = rowdata[column]
            }else{
                acc[column]=null
            }
            return acc;
        },{})

        tabledata.push(newrow)
        fs.writeFileSync(tablepath,JSON.stringify(tabledata))
    }

    Query(databaseName,tablename,QUERY_FUNCTION){
        const tablepath = path.join(databaseName,`${tablename}.json`)

        if (!fs.existsSync(databaseName)||!fs.existsSync(tablepath)){
            throw new Error("request does not exists")
        }

        const readtable = JSON.parse(fs.readFileSync(tablepath))
        const filters = readtable.filter(QUERY_FUNCTION)
        return filters
    }
    update(databaseName,tablename,QUERY_FUNCTION,updatevalues){
        
        const tablepath = path.join(databaseName,`${tablename}.json`)
        
        if (!fs.existsSync(databaseName)||!fs.existsSync(tablepath)){
            throw new Error("request does not exists")
        }

        const readtable = JSON.parse(fs.readFileSync(tablepath))

        readtable.forEach(row => {
            if (QUERY_FUNCTION(row)) {
                Object.keys(updatevalues).forEach(key=>{
                    row[key] = updatevalues[key]
                })
            }
        })

        fs.writeFileSync(tablepath,JSON.stringify(readtable))
    }
    delete(databaseName,tablename,QUERY_FUNCTION){
        const tablepath = path.join(databaseName,`${tablename}.json`)

        if (!fs.existsSync(databaseName)||!fs.existsSync(tablepath)){
            throw new Error("request does not exists")
        }

        let readtable = JSON.parse(fs.readFileSync(tablepath))

        const filterdelete = readtable.filter(row=> !QUERY_FUNCTION(row))

        readtable = filterdelete;
        fs.writeFileSync(tablepath,JSON.stringify(readtable))
    }
    

}

// const db = new Database()

// db.createcluster('mydatabase2')

// db.createSchema('mydatabase2','user','user',['name','email','age'])


// db.insert('mydatabase2','user','user',{name : 'mehul',email:'mehul@mail.com',age:17})


// const Q = db.Query('mydatabase2','user',()=>true)
// console.log(Q);

// db.update('mydatabase2','user',row=>row.name ==='mehul',{email:'mehul1234@mail.com'})


// db.delete('mydatabase2','user',user=>user.name === 'mehul')






