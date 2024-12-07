//	---------------------------------------------------------------------//
//	            	                                                     //
// 		COPYRIGHT 2003-2021 TRIMBLE INC.                                 //
//      		                                                         //
// 		THIS SOFTWARE IS PROPRIETARY INFORMATION OF TRIMBLE INC.         //
// 		AND MAY ONLY BE USED CONSISTENT WITH THE LICENSE GRANTED.        //
//                                                                 		 //
//-----------------------------------------------------------------------//
//                                                                       //
// CUSTOM Program Modification Index                                     //
//                                                           	         //
// 		 Date  Pgmr  Project Number ---------- Description --------------//
// 		------ ----- -------------- -------------------------------------//
//-----------------------------------------------------------------------//
//-----------------------------------------------------------------------//
//      	                                                             //
// 		STANDARD Program Modification Index                              //
//      	                                                             //
//  	Date  Pgmr  Project Number ---------- Description -------------- //
// 		------ ----- -------------- -------------------------------------//
// 		072920 SMIS  IN25572        FleetConneX                          //
//A0001 072923 SMIS  IN44355        Add error logging to fcxerrlogp      //
//A0002 072623 SMIS  IN44355        Fix apos error when writing to dtaq  //
//A0003 090423 SMIS  IN45182        Add datadog logging capability       //
//A0004 110623 SMIS  IN45988        Enahnce datadog logging capability   //
//A0005 050624 SMIS  IN48209        Add lib, ver and customer to dd tag logging
//A0006 102224 SMIS  IN49962        Fix issue with completing messages   //
//-----------------------------------------------------------------------//

const {
  delay,
  ServiceBusClient,
  ServiceBusMessage,
  isServiceBusError,
} = require("@azure/service-bus");
const axios = require("axios");
const { Connection, Statement, IN, CHAR } = require("idb-pconnector");
const config = require("./config.json");
const os = require("os"); //A0004
const { createLogger, format, transports } = require("winston"); //A0003

async function main() {
  //*******************************************************A0003 *start
  const ddLoggingOn = await getDataDogStatus();
  console.log("DataDog logging status:", ddLoggingOn); // A0006
  let logger;
  if (ddLoggingOn) {
    logger = await getDataDogLogger();
    console.log("DataDog logger initialized"); // A0006
    custId = await getCustomerId();   //A0004
    console.log("Customer ID:", custId); // A0006
    custInfo = await getLoggingData();  //A0005
    console.log("Customer Info:", custInfo); // A0006

  }

  //*******************************************************A0003 *end
  let sig = await getSas();
  let connectionString = config.prod.connectUrl + `${sig}`;

  // create a Service Bus client using the connection string to the Service Bus namespace
  let sbClient = new ServiceBusClient(connectionString);
  console.log("sbClient initialized"); // A0006

  // createReceiver() can also be used to create a receiver for a queue.
  let receiver = sbClient.createReceiver(
    config.prod.topicName,
    config.prod.subscriptionName
  );
  console.log("Receiver created"); // A0006

  while (true) {
    try {
      console.log("Receive messages..."); // A0006
      const messages = await receiver.receiveMessages(config.prod.batchCount, {
        maxWaitTimeInMs: config.prod.waitTime,
      });
      //D0003 DebugLog("Count of mesgs recv'd: " + messages.length, "");   //A0001
      DebugLog("Count of mesgs recv'd: " + messages.length, "", false); //A0003
      for (const message of messages) {       //A0006
        try {                               //A0006
        //D0006 await messages.forEach( async (message) => {
        //D0001 console.log(`Received message: ${JSON.stringify(message.body)}`);
        //D0003 DebugLog('Recv New Message', JSON.stringify(message.body));  //A0001
        DebugLog("Recv New Message", JSON.stringify(message.body), true); //A0003
        console.log("Message received..."); // A0006
       // DebugLog("Message id count", `Message Id delivery count: ${message.messageId}: ${message.deliveryCount}`, true); // A0006
       //A0006 *start
        try {
               await receiver.completeMessage(message);
                console.log(`Message Completed: ${message.messageId}`); // A0006
            } catch (error) {
                DebugLog("Error completing message", error.message, true);
            }
        await processMessage(message); //A0006
      } catch (error) {
        DebugLog("Error processing message", error.message, true);
      }
       //A0006 *end
        //D0001 processMessage(message).catch((err) =>
        // D0006 processMessage(message).catch(
        //D0006  (
        //D0006    error //A0001
        //D0006  ) => {
            //D0001 console.log("Error occurred: ", err);
            //D0003 DebugLog("Error processMessage", error.message);  //A0001
        //D0006    DebugLog("Error processMessage", error.message, true); //A0003
        //D0006    process.exit(1);
        //D0006  }
        //D0006);
      //D0006 });
    }
    
    } catch (error) {
      //D0001 console.log(`Error from source occurred: `, error);
      //D0003 DebugLog(`Error from source occurred`, error.message);    //A0001
      DebugLog(`Error from source occurred`, error.message, true); //A0003
      if (isServiceBusError(error)) {
        switch (error.code) {
          case "UnauthorizedAccess":
            if (error.message.includes("ExpiredToken")) {
              //D0001 console.log('Signature expired, reestablishing connection');
              //D0003 DebugLog('Signature expired, renewing...', '');   //A0001
              DebugLog("Signature expired, renewing...", "", false); //A0003

              sig = await getSas();
              let connectionString = config.prod.connectUrl + `${sig}`;

              // create a Service Bus client using the connection string to the Service Bus namespace
              sbClient = new ServiceBusClient(connectionString);

              // createReceiver() can also be used to create a receiver for a queue.
              receiver = sbClient.createReceiver(
                config.prod.topicName,
                config.prod.subscriptionName
              );
            } else {
              //D0001 console.log(`An unrecoverable error occurred. Stopping processing. ${error.code}`, error);
              //D0003 DebugLog(`An unrecoverable error occurred. Stopping processing`, error.message);  //A0001
              DebugLog(
                `An unrecoverable error occurred. Stopping processing`,
                error.message,
                true
              ); //A0003
            }
            break;
          case "ServiceBusy":
            // choosing an arbitrary amount of time to wait.
            //D0001 console.log('Retry...')
            //D0003 DebugLog('Retry...', "ServiceBusy");  //A0001
            DebugLog("Retry...", "ServiceBusy", false); //A0003
            await delay(config.prod.retryTime);
            break;
          default: //A0001
            //D0003 DebugLog("unknown error code from svc bus", error.message);   //A0001
            DebugLog("unknown error code from svc bus", error.message, true); //A0003
            break; //A0001
        }
      }
    }
  }
  //D0003 }

  async function getSas() {
    var options = {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: config.prod.sasAuth,
      },
      body: "grant_type=client_credentials&audience=sas_token",
    };

    try {
      const res = await axios.post(config.prod.sasUrl, options.body, {
        headers: options.headers,
      });

      if (!(res.data && res.data.access_token)) {
        throw new Error(
          `Unexpected SAS response format: ${JSON.stringify(res.data)}`
        );
      }
      console.log('saas token: ', res.data.access_token); //A0006
      return res.data.access_token;
    } catch (error) {
      //D0001  console.log('Error getting SAS; ', error);
      //D0003 DebugLog('Error getting SAS; ', error.message);   //A0001
      DebugLog("Error getting SAS; ", error.message, true); //A0003
    }
  }

  async function processMessage(message) {
    //let outstring = message.body.data;
    let outstring = message.body.data.replace(/'/g, ""); //A0002

    const statement = new Statement(connection);
    const sql = `call qsys2.SEND_DATA_QUEUE(MESSAGE_DATA => '${outstring}', DATA_QUEUE => '${config.prod.ibmIDtaq}',  DATA_QUEUE_LIBRARY => '${config.prod.ibmIDataLib}');`;
    //D0003 DebugLog('exec', sql);   //A0001
    DebugLog("exec", sql, false); //A0003

    try {
      const results = await statement.exec(sql);
      console.log("Message Processed..."); // A0006
    } catch (err) {
      //D0001  console.error(`Error: ${err.stack}`);
      //D0003 DebugLog("processMessage error:", err.message);  //A0001
      DebugLog("processMessage error:", err.message, true); //A0003
    }
  }

  //**********************************************************A0001 *Start
  //D0003 async function DebugLog(info, inMsg) {
  async function DebugLog(info, inMsg, logToDataDog) {
    //A0003

    // write to console
    console.log(`[${info}] : [${inMsg}]`);

    const LogToDb = false;

    if (LogToDb) {
      // write same thing to DB
      const statement = new Statement(connection);
      const sql = `insert into ${config.prod.ibmIDataLib}.fcxerrlogp (fceerrmsg, fcerawmsg) values(?, ?) with nc;`;

      try {
        await statement.prepare(sql);
        await statement.bindParameters([info, inMsg]);
        await statement.exec(sql);
      } catch (err) {
        console.error(`Error: ${err.stack}`);
      }
    }
    //*******************************************************A0003 *start
    if (ddLoggingOn && logToDataDog) {
       //D0004 logger.log("info", inMsg);
        let hostName = os.hostname().toString() + '-' + custId;  //A0004
      //D0005 logger.log("info", inMsg, {host: hostName.trim(), CustomerId: custId});  //A0004
      logger.log("info", inMsg, {host: hostName.trim(), CustomerId: custId, name: custInfo.compName, lib: custInfo.compLib, iesversion: custInfo.compVer});  //A0005
      
    }
    //*******************************************************A0003 *end
  }
  //***********************************************************A0001 *End

  //********************************************************A0003 *start
  async function getDataDogLogger() {
    try {
      const dataDogHttp = `select * from ${config.prod.ibmIDataLib}.WEBSRVCNCT where WSVENDRID = 'DATADOG' and WSVENDRDSC = 'DATALOGGING'`;
      const statement = new Statement(connection);
      await statement.prepare(dataDogHttp);
      await statement.execute();
      const results = await statement.fetchAll();
      
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
      });
    } catch (err) {
      DebugLog("DataDog config error:", err.message, true);
    }
  }

  async function getDataDogStatus() {
    try {
      const sql = `select WSCLIENTID from ${config.prod.ibmIDataLib}.WEBSRVCNCT where WSVENDRID = 'DATADOG' and WSVENDRDSC = 'DATALOGGING'`;
      const statement = new Statement(connection);
      await statement.prepare(sql);
      await statement.execute();
      const results = await statement.fetchAll();
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
  //*******************************************************Begin A0004
  async function getCustomerId() {
   //Add query to pull customer code
   try{
     const custIdQuery = `select substr(data_area_value, 11,3) as custId from TABLE(qsys2.data_area_info('E##DTA', '${config.prod.ibmIDataLib}'))`;
     const custStmt = new Statement(connection);
     await custStmt.prepare(custIdQuery);
     await custStmt.execute();
     const custResults = await custStmt.fetchAll();
     const custId = custResults[0].CUSTID.trim();
     return custId;
   } catch (err) {
    DebugLog("Customer Id fetch error:", err.message, true);
   }

  }
  //*******************************************************End A0004

    //*******************************************************Begin A0005
    async function getLoggingData() {
      //Query to pull customer information
      try{

        const custInfoQuery = `WITH compInfo AS ( SELECT SUBSTR(data_area_value, 4, 30) compName, SUBSTR(data_area_value, 751, 10) compLib FROM TABLE ( qsys2.data_area_info('COMPAN', '${config.prod.ibmIDataLib}') ) ) SELECT compname, complib FROM compinfo;`;
        const custInfoStmt = new Statement(connection);
        await custInfoStmt.prepare(custInfoQuery);
        await custInfoStmt.execute();
        const custInfoResults = await custInfoStmt.fetchAll();
        const compName = custInfoResults[0].COMPNAME.trim();
        const compLib = custInfoResults[0].COMPLIB.trim();
        //const compVer = custInfoResults[0].COMPVER.trim();
        const verInfoQuery = `SELECT SUBSTR(data_area_value, 1, 10) compVer FROM TABLE ( qsys2.data_area_info('ICCVER', '${config.prod.ibmIDataLib}') ) ;`;
        const verInfoStmt = new Statement(connection);
        await verInfoStmt.prepare(verInfoQuery);
        await verInfoStmt.execute();
        try{
        const verInfoResults = await verInfoStmt.fetchAll();
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
     //*******************************************************End A0005
} //A0003

//*******************************************************A0003 *end
const connection = new Connection({ url: "*LOCAL" });

main().catch((err) => {
  //D0001 console.log("Error occurred: ", err);
  //D0003 DebugLog("Main - Error occurred: ", err.message);  //A0001
  console.log("Main - Error occurred", err.message); //A0003
  //D0003 process.exit(1);
});
