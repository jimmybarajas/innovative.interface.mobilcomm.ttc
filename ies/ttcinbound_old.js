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
//A0002 072623 SMIS  INXXXXX        Fix apos error when writing to dtaq  //
//-----------------------------------------------------------------------//

const { delay, ServiceBusClient, ServiceBusMessage, isServiceBusError } = require("@azure/service-bus");
const axios = require('axios');
const { Connection, Statement, IN, CHAR } = require('idb-pconnector');
const config = require('./config.json');


 async function main() 
 {  
 	let sig = await getSas();
	let connectionString = config.prod.connectUrl + `${sig}`;

	// create a Service Bus client using the connection string to the Service Bus namespace
	let sbClient = new ServiceBusClient(connectionString);

	// createReceiver() can also be used to create a receiver for a queue.
	let receiver = sbClient.createReceiver(config.prod.topicName, config.prod.subscriptionName);

	while (true) 
	{
		try
		{
			const messages = await receiver.receiveMessages(config.prod.batchCount, {maxWaitTimeInMs: config.prod.waitTime});
			DebugLog("Count of mesgs recv'd: " + messages.length, "");   //A0001
			messages.forEach(message => 
			{
				//D0001 console.log(`Received message: ${JSON.stringify(message.body)}`);
				DebugLog('Recv New Message', JSON.stringify(message.body));  //A0001
				
                receiver.completeMessage(message);
                //D0001 processMessage(message).catch((err) => 
                processMessage(message).catch((error) =>                //A0001
				{
					//D0001 console.log("Error occurred: ", err);
					DebugLog("Error processMessage", error.message);  //A0001
					process.exit(1);
				});;
			});
		} 
		catch (error) 
		{		
			//D0001 console.log(`Error from source occurred: `, error);	
			DebugLog(`Error from source occurred`, error.message);    //A0001
			if (isServiceBusError(error)) 
			{
				switch (error.code) 
				{
					case 'UnauthorizedAccess':
						if (error.message.startsWith('ExpiredToken')) 
						{
							//D0001 console.log('Signature expired, reestablishing connection');
							DebugLog('Signature expired, renewing...', '');   //A0001
							
							sig = await getSas();
							let connectionString = config.prod.connectUrl + `${sig}`;
							
							// create a Service Bus client using the connection string to the Service Bus namespace
							sbClient = new ServiceBusClient(connectionString);

							// createReceiver() can also be used to create a receiver for a queue.
							receiver = sbClient.createReceiver(config.prod.topicName, config.prod.subscriptionName);
						} 
						else 
						{
							//D0001 console.log(`An unrecoverable error occurred. Stopping processing. ${error.code}`, error);
							DebugLog(`An unrecoverable error occurred. Stopping processing`, error.message);  //A0001	
						}
						break;
					case "ServiceBusy":
						// choosing an arbitrary amount of time to wait.
						//D0001 console.log('Retry...')
						DebugLog('Retry...', "ServiceBusy");  //A0001
						await delay(config.prod.retryTime);
						break;
                    default:        //A0001
                        DebugLog("unknown error code from svc bus", error.message);   //A0001    
                        break;    //A0001
						
				}
			}
		}
	}
}

async function getSas() 
{
	var options = 
	{
		headers: 
		{
      			'Content-Type': 'application/x-www-form-urlencoded',
      			 Authorization: config.prod.sasAuth
   		},
    		body: 'grant_type=client_credentials&audience=sas_token'
  	};

	try 
	{
		const res = await axios.post(config.prod.sasUrl, options.body,
		{
      			headers: options.headers,
		});

		if (!(res.data && res.data.access_token)) 
		{
			throw new Error(`Unexpected SAS response format: ${JSON.stringify(res.data)}`);
		}

		return res.data.access_token;
    
	} 
	catch (error) 
	{
	   //D0001  console.log('Error getting SAS; ', error);
		DebugLog('Error getting SAS; ', error.message);   //A0001											
  	}
}

 async function processMessage(message) {
    //let outstring = message.body.data;
	let outstring = message.body.data.replace(/'/g, "");   //A0002

	const statement = new Statement(connection);
	const sql = `call qsys2.SEND_DATA_QUEUE(MESSAGE_DATA => '${outstring}', DATA_QUEUE => '${config.prod.ibmIDtaq}',  DATA_QUEUE_LIBRARY => '${config.prod.ibmIDataLib}');`;
	DebugLog('exec', sql);   //A0001
	
	try {

		 const results = await statement.exec(sql);

	 }
 
	 catch (err) {
		 //D0001  console.error(`Error: ${err.stack}`);
		 DebugLog("processMessage error:", err.message);
	 };

  }

  //**********************************************************A0001 *Start
 async function DebugLog(info, inMsg) {	

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
	        }
	        catch (err) {
	            console.error(`Error: ${err.stack}`);
	        };
	    }

	    const LogToDataDog = false;
	    
	    if (LogToDataDog) {
	        
	    }
	  }
  //***********************************************************A0001 *End
   
  
const connection = new Connection({ url: '*LOCAL' });

main().catch((err) => 
{
	//D0001 console.log("Error occurred: ", err);
	DebugLog("Main - Error occurred: ", err.message);  //A0001
	process.exit(1);
});
