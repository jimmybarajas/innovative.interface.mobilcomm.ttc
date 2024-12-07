//	---------------------------------------------------------------------//
//	            	                                                       //
// 		COPYRIGHT 2003-2021 TRIMBLE INC.                                   //
//      		                                                             //
// 		THIS SOFTWARE IS PROPRIETARY INFORMATION OF TRIMBLE INC.           //
// 		AND MAY ONLY BE USED CONSISTENT WITH THE LICENSE GRANTED.          //
//                                                                 		   //
//-----------------------------------------------------------------------//
//                                                                       //
// CUSTOM Program Modification Index                                     //
//                                                           	           //
// 		 Date  Pgmr  Project Number ---------- Description --------------  //
// 		------ ----- -------------- -------------------------------------  //
//-----------------------------------------------------------------------//
//-----------------------------------------------------------------------//
//      	                                                               //
// 		STANDARD Program Modification Index                                //
//      	                                                               //
//  	Date  Pgmr  Project Number ---------- Description --------------   //
// 		------ ----- -------------- ---------------------------------------//
// 		070724 SMIS                 Init create                            //
//-----------------------------------------------------------------------//

const { Connection, Statement, IN, CHAR, DBPool} = require("idb-pconnector");
const { Kafka, logLevel } = require('kafkajs')
const config = require("./ttconfig.json");
const os = require("os");
require('dotenv').config();
const { createLogger, format, transports } = require("winston");

async function main() {
  
  const connSettings = await getConn();
  const ddLoggingOn = await getDataDogStatus();
  console.log("DataDog logging status:", ddLoggingOn);

  let logger;
  if (ddLoggingOn) {
    logger = await getDataDogLogger();
    console.log("DataDog logger initialized");
    custId = await getCustomerId();
    console.log("Customer ID:", custId); 
    custInfo = await getLoggingData();  
    console.log("Customer Info:", custInfo);
  }

  const clientId = connSettings.clientId;
  const brokers = [connSettings.brokers];
  const topic = connSettings.topic;
  const kafka = new Kafka({clientId, brokers, ssl: true, sasl: {mechanism: 'plain', username: connSettings.key, password: connSettings.secret} });
  console.log(kafka);
  const consumer = kafka.consumer({groupId: connSettings.groupId});
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  console.log(consumer);
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('Received message', {
        topic,
        partition,
        value: message.value.toString()
      });
      await processMessage(message.value);
    }
  });
//};
  
  async function processMessage(message) {
    let outstring = JSON.stringify(JSON.parse(message)).trim(); 
    outstring = JSON.stringify(JSON.parse(outstring));
    const sql = `call qsys2.SEND_DATA_QUEUE(MESSAGE_DATA => '${outstring}', DATA_QUEUE => '${config.prod.ibmIDtaq}',  DATA_QUEUE_LIBRARY => '${config.prod.ibmIDataLib}')`;
    DebugLog("exec", sql, false, '');

    try {
      const connection = await pool.attach();
      const statement = connection.getStatement();
      const results = await statement.exec(sql);
      await pool.detach(connection);
      console.log("Message Processed...");
    } catch (err) {
      DebugLog("processMessage error:", err.message, true, 'error');
    }
  }

  async function DebugLog(info, inMsg, logToDataDog, severity) {
    // write to console
    console.log(`[${info}] : [${inMsg}]`);
    
       if (ddLoggingOn && logToDataDog) {
        let hostName = os.hostname().toString() + "-" + custId;
        logger.log(severity, inMsg, {
          host: hostName.trim(),
          CustomerId: custId,
        });
     
    }
  }
  async function getConn() {
    try {
      const connection = await pool.attach();
      const statement = connection.getStatement();    
      const sql = `select * from ${config.prod.ibmIDataLib}.WEBSRVCNCT where WSVENDRID = 'TTC' and WSVENDRDSC = 'TRLR_TRACK'`;
      await statement.prepare(sql);
      await statement.execute();
      const results = await statement.fetchAll();
      await pool.detach(connection);

      const clientId = results[0].WSCLIENTID.trim();
      const brokers = results[0].WSURLENDP.trim();
      const groupId = results[0].WSNAMESPC.trim();
      const topic = results[0].WSSRVNAME.trim();
      const key = results[0].WSUSERNAME.trim();
      const secret = results[0].WSACCESSTK.trim();

            
      return {
        clientId: clientId,
        brokers: brokers,
        groupId: groupId,
        topic : topic,
        key : key,
        secret : secret,

      };
    } catch (err) {
      DebugLog("No Configuration Error:", err.message, true);
    }
  }
    async function getDataDogLogger() {
      try {
        const connection = await pool.attach(); //D0007
        const statement = connection.getStatement();  
        const sql = `select * from ${config.prod.ibmIDataLib}.WEBSRVCNCT where WSVENDRID = 'DATADOG' and WSVENDRDSC = 'DATALOGGING'`;
        await statement.prepare(sql);
        await statement.execute();
        const results = await statement.fetchAll();
        await pool.detach(connection);
        if (results.length == 1) {
             
        const nameSpace = results[0].WSNAMESPC.trim();
        const host = results[0].WSURLENDP.trim();
        const secret = results[0].WSCLSECRET.trim();
        const serviceName = results[0].WSSRVNAME.trim();
        const httpTransportOptions = {
          host: host,
          path:
            nameSpace +
            "dd-api-key=" +
            secret +
            "&ddsource=nodejs&service=" +
            serviceName,
          ssl: true,
        };

        return createLogger({
          level: "info",
          exitOnError: false,
          format: format.json(),
          transports: [new transports.Http(httpTransportOptions)],
        })
      };
      } catch (err) {
        DebugLog("DataDog config error:", err.message, true);
      }
    }
  
  async function getDataDogStatus() {
    try {
        const connection = await pool.attach(); //D0007
        const statement = connection.getStatement();   
      const sql = `select WSCLIENTID from ${config.prod.ibmIDataLib}.WEBSRVCNCT where WSVENDRID = 'DATADOG' and WSVENDRDSC = 'DATALOGGING'`;
      await statement.prepare(sql);
      await statement.execute();
      const results = await statement.fetchAll();
      await pool.detach(connection);
      console.log(results);
      if (results.length == 1) {
        return results[0].WSCLIENTID.trim();
      } else {
        return false;
      }
    } catch (err) {
      DebugLog("DataDog config error:", err.message, true);
      return false;
    }
  }

  async function getCustomerId() {
    //Add query to pull customer code
    try {
        const connection = await pool.attach(); //D0007
        const statement = connection.getStatement(); 
      const custIdQuery = `select substr(data_area_value, 11,3) as custId from TABLE(qsys2.data_area_info('E##DTA', '${config.prod.ibmIDataLib}'))`;
      await statement.prepare(custIdQuery);
      await statement.execute();
      const custResults = await statement.fetchAll();
      const custId = custResults[0].CUSTID.trim();
      await pool.detach(connection);
      if (custResults.length == 1) {
        return custResults[0].CUSTID.trim();
      }
      else {
        return 'saas';
      }
    } catch (err) {
      DebugLog("Customer Id fetch error:", err.message, true);
    }
  }
  async function getLoggingData() {
    //Query to pull customer information
    try{
      const connection = await pool.attach(); 
      const statement = connection.getStatement(); 
      const custInfoQuery = `WITH compInfo AS ( SELECT SUBSTR(data_area_value, 4, 30) compName, SUBSTR(data_area_value, 751, 10) compLib FROM TABLE ( qsys2.data_area_info('COMPAN', '${config.prod.ibmIDataLib}') ) ) SELECT compname, complib FROM compinfo;`;
      
      await statement.prepare(custInfoQuery);
      await statement.execute();
      const custInfoResults = await statement.fetchAll();
      await pool.detach(connection);
      const compName = custInfoResults[0].COMPNAME.trim();
      const compLib = custInfoResults[0].COMPLIB.trim();
      
      const connection2 = await pool.attach(); 
      const verInfoStmt = connection2.getStatement(); 
      const verInfoQuery = `SELECT SUBSTR(data_area_value, 1, 10) compVer FROM TABLE ( qsys2.data_area_info('ICCVER', '${config.prod.ibmIDataLib}') ) ;`;
      
      await verInfoStmt.prepare(verInfoQuery);
      await verInfoStmt.execute();
      try{
      const verInfoResults = await verInfoStmt.fetchAll();
      await pool.detach(connection2);
      const compVer = verInfoResults[0].COMPVER.trim();
      let custData = {
        compName: compName,
        compLib: compLib,
        compVer: compVer
      }
      return custData;
    }
      catch {
        compVer = 'Saas';
      }
      let custData = {
        compName: compName,
        compLib: compLib,
        compVer: compVer
      };
      return custData;
    } catch (err) {

     DebugLog("Customer Data fetch error:", err.message, true);

    }


   }
}

const pool = new DBPool({
    url: '*LOCAL',
    maxPoolSize: 10, // Maximum number of connections in the pool
  });

main().catch((err) => {
  console.log("Main - Error occurred", err.message);
  });
