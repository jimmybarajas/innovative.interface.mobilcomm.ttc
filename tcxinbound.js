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
const { createLogger, format, transports } = require("winston"); //A0003

async function main() {
  //*******************************************************A0003 *start
  const ddLoggingOn = await getDataDogStatus();
  let logger;
  if (ddLoggingOn) {
    logger = await getDataDogLogger();
  }

  //*******************************************************A0003 *end
  let sig = await getSas();
  let connectionString = config.prod.connectUrl + `${sig}`;

  // create a Service Bus client using the connection string to the Service Bus namespace
  let sbClient = new ServiceBusClient(connectionString);

  // createReceiver() can also be used to create a receiver for a queue.
  let receiver = sbClient.createReceiver(
    config.prod.topicName,
    config.prod.subscriptionName
  );

  while (true) {
    try {
      const messages = await receiver.receiveMessages(config.prod.batchCount, {
        maxWaitTimeInMs: config.prod.waitTime,
      });
      //D0003 DebugLog("Count of mesgs recv'd: " + messages.length, "");   //A0001
      DebugLog("Count of mesgs recv'd: " + messages.length, "", false); //A0003
      messages.forEach((message) => {
        //D0001 console.log(`Received message: ${JSON.stringify(message.body)}`);
        //D0003 DebugLog('Recv New Message', JSON.stringify(message.body));  //A0001
        DebugLog("Recv New Message", JSON.stringify(message.body), true); //A0003

        receiver.completeMessage(message);
        //D0001 processMessage(message).catch((err) =>
        processMessage(message).catch(
          (
            error //A0001
          ) => {
            //D0001 console.log("Error occurred: ", err);
            //D0003 DebugLog("Error processMessage", error.message);  //A0001
            DebugLog("Error processMessage", error.message, true); //A0003
            process.exit(1);
          }
        );
      });
    } catch (error) {
      //D0001 console.log(`Error from source occurred: `, error);
      //D0003 DebugLog(`Error from source occurred`, error.message);    //A0001
      DebugLog(`Error from source occurred`, error.message, true); //A0003
      if (isServiceBusError(error)) {
        switch (error.code) {
          case "UnauthorizedAccess":
            if (error.message.startsWith("ExpiredToken")) {
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

    const LogToDb = true;

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
        logger.log("info", inMsg);
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
} //A0003

//*******************************************************A0003 *end
const connection = new Connection({ url: "*LOCAL" });

main().catch((err) => {
  //D0001 console.log("Error occurred: ", err);
  //D0003 DebugLog("Main - Error occurred: ", err.message);  //A0001
  console.log("Main - Error occurred", err.message); //A0003
  //D0003 process.exit(1);
});
