<?php

	/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
	 ** Infrastructure for processing ODS Call History and
	 **   Agent State Data ETL into Monet Service.
	 **
	 **
	 **
	 **
	 ** ** ** ** **/
	App::uses( 'ConsoleInput', 'Console' );
	App::uses( 'ConsoleOutput', 'Console' );

	class DoMonetShell extends AppShell {

		public $uses = array( 'CallDetail', 'AgentStateDetail', 'QSuite' );  	// Access to CallDetail & AgentStatus Models

		private $AuthenticationInfo = array( "userName" => "Connexion Point", "password" => "591255076" );

		private $NumACDRecordsSent = 0;
		private $TotACDRecordsSent = 0;
		private $NumAgentStatsSent = 0;
		private $TotAgentStatsSent = 0;
		private $NumETLRecordsSent = 0;
		private $TotETLRecordsSent = 0;

		private $ConsoleMsgs		= array( "", "", "", "", "" );		// Console "screen"
		private $NumMsgs				= 4;															// Adjust for origin
		private $ErrorMsg				= "";															// Console error message
		private $ExceptionMsg		= "";															// Exception message

 		const 	SQLSTATE_23000	= 23000;													// MySQL duplicate record exception code

 		private $LastEndtime		= "2014-06-12 13:00:00";					// Endtime of last call record processed

		private $UnitTestFlg		= false;													// Unit testing flag
																															// 		t = Toggle [u]nit [t]esting
		private $UnitTesting		= "n";														// Test specific area
																															// 		a = Simulate [a]uthentication failure
																															// 		h = Simulate acd [h]istory update failure
																															//		s = Simulate agent [s]tate update failure
																															//		n = Return to [n]ormal operational state



		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 ** Default entry point for CAKE shell classes
		 **
		 ** @params	Nothing
		 ** @return	Nothing
		 ** ** ** ** **/
		public function main() {

		 	$this->MainProcess();

		}



		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 ** Framework for ODS ETL into Monet Service
		 **
		 ** @params: 	$argc[], $argv[]
		 ** @return:	Nothing
		 ** ** ** ** **/
	 	public function MainProcess() {

	 		$Jogging				= true;		// Main process execution state

	  	$UpdateACD			= 5;			// Every 15 minutes (recommended)
	  	$CheckMonet			= 30;			// Every 60 seconds (recommended)
	  	$UpdateStatus		= 10;			// Every 1 second   (recommended)
	  	$ProcessETL			= 5;			// Every 5 seconds

			$ConsoleInput		= new ConsoleInput();


			if ( $this->UnitTestFlg ) {
		  	$this->AddConsoleMsg( "Main - Begin (". date("m/d/y, h:m:s", time()) .")[".
		  		$UpdateACD ."m][". $CheckMonet ."s][". $UpdateStatus ."s]" );
		  }



			/***** ***** ***** ***** ***** ***** ***** ***** *****/
	  	while ( $Jogging ) {

				if  ( !$ConsoleInput->dataAvailable() ) {

		  		if ( !($mins = localtime(time(),true)['tm_min'] % $UpdateACD) && (($secs = localtime(time(),true)['tm_sec']) < 2) ) {

						// Send ACD history records
			    	$time = microtime(true);
			    	$num = $this->SendAcdData();
			    	$this->AddConsoleMsg( "Sent ACD Records [" . $num . "]" . (($this->UnitTestFlg) ? ("  [" . $mins . "][" . $secs . "][" . sprintf("%.2f", (microtime(true) - $time)) . "][" . date("H:i:s",time()) . "]") : "") );

			    } elseif ( !($secs = localtime(time(),true)['tm_sec'] % $CheckMonet) ) {

						// Check Monet for updates
			    	$time = microtime(true);
			    	$num = $this->CheckMonet();
			    	$this->AddConsoleMsg( "Check Monet [" . $num . "]" . (($this->UnitTestFlg) ? ("  [" . $mins . "][" . $secs . "][" . sprintf("%.2f", (microtime(true) - $time)) . "][" . date("i:s",time()) . "]") : "") );

			    } elseif ( !($secs = localtime(time(),true)['tm_sec'] % $UpdateStatus) ) {

						// Update agent status
			    	$time = microtime(true);
			    	$num = $this->UpdateAgentStatus();
			    	$this->AddConsoleMsg( "Updated Agent Status [" . $num . "]" . (($this->UnitTestFlg) ? ("  [" . $mins . "][" . $secs . "][" . sprintf("%.2f", (microtime(true) - $time)) . "][" . date("i:s",time()) . "]") : "") );

			    } elseif ( !($secs = localtime(time(),true)['tm_sec'] % $ProcessETL) ) {

			    	// Pull data from QSuite and push to ODS
			    	$time = microtime(true);
			    	$num = $this->QS_ETL_ODS();
			    	$this->AddConsoleMsg( "QS =ETL=> ODS [" . $num . "]" . (($this->UnitTestFlg) ? ("  [" . $mins . "][" . $secs . "][" . sprintf("%.2f", (microtime(true) - $time)) . "][" . date("i:s",time()) . "]") : "") );

			    }

					$this->UpdateConsole();

					// Relax for a bit..
		  		sleep( 1 );

				} else {

					// $this->out( "[" . $ConsoleInput->read() . "]" );
					$input = $ConsoleInput->read();
					switch ( strtolower($input) ) {
						case "u"	:
						case "t"	:	$this->UnitTestFlg = !$this->UnitTestFlg;
												$this->AddConsoleMsg( "Unit Testing: " . (($this->UnitTestFlg) ? "On" : "Off" ) );
												break;

						case "n"	:	$this->AddErrorMsg( "", false );
						case "a"	:
						case "s"	:
						case "h"	:	$this->UnitTesting = $input;
												break;

						case "x"	:
						case "e"	:
						case "q"	:	$Jogging = false;

						case "b"	: $this->Beep( 3 );
						case ""		:
						default		:	break;
					}

				}

			}  // End main while processing loop


	  	if ( $this->UnitTestFlg ) $this->out( "\nMain - End\n" );
	  } // End Function MainProcess





		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 ** Sends ACD Call History Records to Monet
		 **
		 ** @params: 	Nothing
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function SendAcdData() {

			// if ( $this->UnitTestFlg ) $this->AddConsoleMsg( "Send ACD Data" );
			$StrtTime = microtime(true);


			// Get call history records from ODS
			$SQry =  "SELECT"
						 . "	   campaign_name AS Queue_ID,"
						 . "	   DATE(call_history_date_time) AS Call_Date,"
						 . "	   start_time AS Interval_Start,"
						 . "	   end_time AS Interval_End,"
						 . "	   @Status := ROUND(RAND()) AS Status,"																				/* xXx */
						 . "	   CASE WHEN @Status THEN 1 ELSE 0 END AS Call_Handled,"											/* xXx */
						 . "	   CASE WHEN @Status THEN 0 ELSE 1 END AS Call_Abandoned,"										/* xXx */
						 . "	   CASE WHEN @Status THEN duration ELSE NULL END AS Length_Not_Dropped,"			/* xXx */
						 . "	   CASE WHEN @Status THEN NULL ELSE duration END AS Length_Call_Dropped,"			/* xXx */
						 . "	   ROUND(RAND()*100) AS Queue_Time,"																					/* xXx */
						 . "	   ROUND(RAND()*100) AS acw,"																									/* xXx */
						 . "	   ROUND(RAND()) AS Service_Level"																						/* xXx */
						 . "  FROM cxp_data_warehouse.call_details"
						 . "   ,(SELECT @Status := ROUND(RAND())) vars"
						 . "  WHERE"
						 . "  	    campaign_name IS NOT NULL"
						 . "  	AND campaign_name NOT LIKE '%sample%'";

			$MQry  = "SELECT"
						 . "    Queue_ID AS QueueID,"
						 . "    FLOOR(Interval_Start) AS IntervalStart,"
						 . "    CEILING(Interval_End) AS IntervalEnd,"
						 . "    SUM(Call_Handled) AS HandleCalls,"																					/* x?x */
						 . "    SUM(Call_Handled) AS HandledCalls,"
						 . "    SUM(Call_Handled) AS AnsweredCalls,"																				/* x?x */
						 . "    SUM(Call_Abandoned) AS AbandonedCalls,"
						 . "    IFNULL(ROUND(AVG(Length_Not_Dropped),0),0) AS ATT,"
						 . "    IFNULL(FLOOR(AVG(acw)),0) AS ACW,"
						 . "    IFNULL(ROUND(AVG(Queue_Time),0),0) AS ASA,"
						 . "    IFNULL(ROUND(AVG(Length_Call_Dropped),0),0) AS ATAB,"
						 . "    FLOOR(SUM(Service_Level) / COUNT(*) * 100) AS ServiceLevel,"
						 . "    ROUND(RAND()*000) AS Utilization,"																					/* x?x */
						 . "    ROUND(RAND()*100) AS Overflow,"																							/* xXx */
						 . "    IFNULL(ROUND(AVG(Queue_Time),0),0) AS OverflowDelays,"											/* xXx */
						 . "    ROUND(RAND()*100) AS Staffed"																								/* xXx */
						 . "  FROM ("
						 . 			$SQry
						 . "       ) Call_Details"
						 . "  GROUP BY QueueID, Call_Date"
						 . "  ORDER BY QueueID, Call_Date";


			try {

				// Fetch call details
	    	$ODS_Data = $this->CallDetail->query( $MQry );

	  	} catch (Exception $ex) {

	    	$this->AddExceptionMsg( $ex->getMessage(), true );
	    	return;		// Unrecoverable

			}


			if ( sizeof( $ODS_Data ) ) {

				// Process each call history record
				foreach ( $ODS_Data as $record ) {

					if ( $this->UnitTestFlg ) {
						// Override these fields for unit testing
						$record['Call_Details']['QueueID'] = 'WFM - Test';
						$DateStart = strtotime("05/20/14, 06:00:00") + (15 * 60 * 4);
						$DateEnd   = strtotime("05/20/14, 06:00:00") + (15 * 60 * (4 + 1));
					} else {
						$record['Call_Details']['QueueID'] = 'WFM - Test';
						$DateStart = strtotime("05/20/14, 06:00:00") + (15 * 60 * 4);
						$DateEnd   = strtotime("05/20/14, 06:00:00") + (15 * 60 * (4 + 1));
					}

					// Set timeslice
					$SliceStart = date("Y-m-d",$DateStart) ."T". date("H:i:s",$DateStart) . "+01:00";
					$SliceEnd   = date("Y-m-d",$DateEnd)   ."T". date("H:i:s",$DateEnd)   . "+01:00";
					$record[0]['IntervalStart'] = $SliceStart;
					$record[0]['IntervalEnd'] 	= $SliceEnd;
		/*
					// Diagnostics
					if ( $this->UnitTestFlg ) {
						foreach ( $record['Call_Details'] as $k => $v )	$this->AddConsoleMsg( $k . " = " . $v . "" );
						if ( isset($record[0]) ) foreach ( $record[0] as $k => $v ) $this->AddConsoleMsg( $k . " = " . $v . "" );
						$this->AddConsoleMsg( "" );
					}
		*/
					// Build ACD call records collection
					$AcdHistoryRecordsArray['acdHistoryCollection']['ACDHistoryObj'][] = array_merge( $record['Call_Details'], $record[0] );
				}

				// Add authentication info to call records array
				$AcdHistoryRecordsArray['userName'] = $this->AuthenticationInfo['userName'];
				$AcdHistoryRecordsArray['password'] = $this->AuthenticationInfo['password'];

				// Authenticate to Monet service
				$SC_Monet = new SoapClient( "https://www.wfmlive.com/datacollector/monetwebservice.asmx?wsdl", array('trace'=>1) );
				// $SC_Monet = new SoapClient( "http://support.monetsoftware.com:20000/DcReceiverWebService.asmx?wsdl", array('trace'=>1) );
				$AuthenticationResult = $SC_Monet->GetAuthentication( $this->AuthenticationInfo );

				if ( $AuthenticationResult->GetAuthenticationResult ) {
	/** /
					// Get general configuration info
					$GetImportResult = $SC_Monet->GetImportParameters( $this->AuthenticationInfo );

					// Get list of queues to track
					$GetAcdResult = $SC_Monet->GetAcdGroupInformation( $this->AuthenticationInfo );
	/ **/
					// Submit ACD history information to Monet service
					$SendAcdResult = $SC_Monet->sendAcdHistoryRecordsArray( $AcdHistoryRecordsArray );
					if (($this->UnitTestFlg) && ($this->UnitTesting == "h")) $SendAcdResult->SendAcdHistoryRecordsArrayResult = false;

					// Check for errors
					if ( !$SendAcdResult->SendAcdHistoryRecordsArrayResult ) {
						$this->AddErrorMsg( "Monet Send ACD Data Error", true );
					}

					$this->TotACDRecordsSent += ($this->NumACDRecordsSent = sizeof( $ODS_Data ));
	/**/
				} else {
					$this->AddErrorMsg( "Monet Authentication Error", true );
				}

			}

			return( sizeof( $ODS_Data ) );
    } // End Function SendAcdData





		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 ** Sends Agent Status Records to Monet
		 **
		 ** @params: 	Nothing
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function UpdateAgentStatus() {

			// if ( $this->UnitTestFlg ) $this->AddConsoleMsg( "Send Agent Status" );
			$StrtTime = microtime(true);


			// Get agent status records from ODS

			$MQry  = "SELECT"
						 . "	   agent_id AS AgentID,"
						 . "	   ustate_name AS Status,"
						 . "	   timestamp AS DateStamp"
						 . "  FROM cxp_data_warehouse.agent_state_details"
						 . "  WHERE"
						 . "  	 agent_id IS NOT NULL";

//						 . "  	AND campaign_name NOT LIKE '%sample%'";
//			$MQry .= "    AND call_history_date_time IS NOT NULL";

			$MQry .= "  LIMIT 3";


			try {

				// Fetch agent state details
	    	$Agent_Data = $this->AgentStateDetail->query( $MQry );

	  	} catch (Exception $ex) {

	    	$this->AddExceptionMsg( $ex->getMessage(), true );
	    	return;		// Unrecoverable

			}

/** /
			$Agent_Data = array(
				'userName' => $this->AuthenticationInfo['userName'],
				'password' => $this->AuthenticationInfo['password'],
				'agentStateCollection' => array(
					array( 'AgentID' => "Test1", 'Status' => "Test", 'DateStamp' => $DateTimeStamp ),
					array( 'AgentID' => "Test2", 'Status' => "Test", 'DateStamp' => $DateTimeStamp )
					)
				);
/ **/

			if ( sizeof( $Agent_Data ) ) {

				// Process each agent state record
				foreach ( $Agent_Data as $record ) {

					// Build agent state records collection
					$DateTime = strtotime( $record['agent_state_details']['DateStamp'] );
					$DateTimeStamp = date("Y-m-d",$DateTime) ."T". date("H:i:s",$DateTime) . "+01:00";
					$record['agent_state_details']['DateStamp'] = $DateTimeStamp;

					$AgentStateRecordsArray['agentStateCollection']['AgentStateObj'][] = array_merge( $record['agent_state_details'] /*, $record[0]*/ );
				}

				// Add authentication info to agent records array
				$AgentStateRecordsArray['userName'] = $this->AuthenticationInfo['userName'];
				$AgentStateRecordsArray['password'] = $this->AuthenticationInfo['password'];

				// Authenticate to Monet service
				$SC_Monet = new SoapClient( "https://www.wfmlive.com/datacollector/monetwebservice.asmx?wsdl", array('trace'=>1) );
				// $SC_Monet = new SoapClient( "http://support.monetsoftware.com:20000/DcReceiverWebService.asmx?wsdl", array('trace'=>1) );
				$AuthenticationResult = $SC_Monet->GetAuthentication( $this->AuthenticationInfo );
				if (($this->UnitTestFlg) && ($this->UnitTesting == "a")) $AuthenticationResult->GetAuthenticationResult = false;

				if ( $AuthenticationResult->GetAuthenticationResult ) {
	/** /
					// Get general configuration info
					$GetImportResult = $SC_Monet->GetImportParameters( $this->AuthenticationInfo );

					// Get list of agents to track
					$GetAgentResult = $SC_Monet->GetAgentInformation( $this->AuthenticationInfo );
	/ **/

					// Submit agent state information to Monet service
					$SendAgentStateResult = $SC_Monet->sendAgentStateRecordsArray( $AgentStateRecordsArray );
					if (($this->UnitTestFlg) && ($this->UnitTesting == "s")) $SendAgentStateResult->SendAgentStateRecordsArrayResult = false;

					// Check for errors
					if ( !$SendAgentStateResult->SendAgentStateRecordsArrayResult ) {
						$this->AddErrorMsg( "Monet Send Agent State Error", true );
						// Debugger::dump( $AgentStateRecordsArray, 9 );  /* debug( $this ); */  exit();
					}

					$this->TotAgentStatsSent += ($this->NumAgentStatsSent = sizeof( $Agent_Data ));

				} else {
					$this->AddErrorMsg( "Monet Authentication Error", true );
				}

			}

			return( sizeof( $Agent_Data ) );
    } // End Function UpdateAgentStatus





		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 ** Check Monet State
		 **
		 ** @params: 	Nothing
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function CheckMonet() {

			// if ( $this->UnitTestFlg ) $this->AddConsoleMsg( "Check Monet State" );
			$StrtTime = microtime(true);



			return( 0 );
    } // End Function CheckMonet





		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 **
		 **
		 ** @params: 	Nothing
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function UpdateConsole() {
			$width = 50;
			$spaces = str_pad("", $width);
			$line   = "+" . str_pad("", $width-2, "-") . "+";
			$title  = "ODS -> Monet ETL Service";
			$exit   = " (Press 'X<Enter>' to terminate process)";

			$ConsoleOutput = new ConsoleOutput();

			$ConsoleOutput->styles('text',  	array('text' => 'white',  'background' => 'black', 'bold' => true));
			$ConsoleOutput->styles('title', 	array('text' => 'green',  'background' => 'black', 'bold' => true));
			$ConsoleOutput->styles('exit',  	array('text' => 'cyan',   'background' => 'black', 'bold' => true));
			$ConsoleOutput->styles('error', 	array('text' => 'white',  'background' => 'red',   'bold' => true));
			$ConsoleOutput->styles('except',	array('text' => 'yellow', 'background' => 'black', 'bold' => true));

			$this->clear();
			$this->out();

 			$this->out( $ConsoleOutput->styleText( "<text>  " . $line . "</text>" ) );
 			$this->out( $ConsoleOutput->styleText( "<text>  |" . substr($spaces, 0, ($width-2 -strlen($title))/2) . "</text><title>" . $title . "</title><text>" . substr($spaces, 0, ($width-2 -strlen($title))/2) . "|</text>" ) );
 			$this->out( $ConsoleOutput->styleText( "<text>  |" . substr($spaces, 0, $width-2) . "|</text>" ) );
 			$this->out( $ConsoleOutput->styleText( "<text>  |" . substr($spaces, 0, ($width-2 -strlen($exit))/2) . "</text><exit>" . $exit . "</exit><text>" . substr($spaces, 0, ($width-2 -strlen($exit))/2) . "|</text>" ) );
 			$this->out( $ConsoleOutput->styleText( "<text>  " . $line . "</text>\n" ) );

 			$this->out( $ConsoleOutput->styleText( "<text>    ACD Records Sent: </text><title>" . $this->NumACDRecordsSent . " / " . $this->TotACDRecordsSent . "</title>" ) );
 			$this->out( $ConsoleOutput->styleText( "<text>    Agent State Sent: </text><title>" . $this->NumAgentStatsSent . " / " . $this->TotAgentStatsSent . "</title>" ) );
 			$this->out( $ConsoleOutput->styleText( "<text>    ETL Records Sent: </text><title>" . $this->NumETLRecordsSent . " / " . $this->TotETLRecordsSent . "</title>\n" ) );

			if ( strlen($this->ErrorMsg) ) {
 				$this->out( $ConsoleOutput->styleText( "    <error>" . substr($spaces, 0, ($width-4 -strlen($this->ErrorMsg))/2) . $this->ErrorMsg . substr($spaces, 0, ($width-4 -strlen($this->ErrorMsg))/2) . "</error>\n" ) );
 			}

			// Messages
			for ( $i=0; $i <= $this->NumMsgs; $i++ ) $this->out( "    " . $this->ConsoleMsgs[$i] );

			if ( strlen($this->ExceptionMsg) ) {
 				$this->out( $ConsoleOutput->styleText( "\n    <error>Exception:</error><except> " . wordwrap( $this->ExceptionMsg, $width-10, "\n    " ) . "</except>\n" ) );
 			}

			$this->out( "\n\n" . /*" [n/a/s/h/t/x]\n" .*/ " :: ", 0 );
		} // End Function ConsoleOutput



		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 **
		 **
		 ** @params: 	$msg  = Message to add to console output.
		 **						$line = Line number to insert msg (0-4).
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function AddConsoleMsg( $msg = "", $line = -1 ) {

			$line = ( ($line < 0) ? $this->NumMsgs : $line );

			if ( $line == $this->NumMsgs )
				for ( $i=0; $i < $this->NumMsgs; $i++ )
					$this->ConsoleMsgs[$i] = $this->ConsoleMsgs[$i+1];				// Scroll lines

			$this->ConsoleMsgs[$line] = $msg;
		}



		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 **
		 **
		 ** @params: 	$msg   = Message to add to console output.
		 **						$state = true: Add message; false: clear msg.
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function AddErrorMsg( $msg = "", $state = false ) {

			$this->ErrorMsg = ( $state ) ? $msg : "";

		}



		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 **
		 **
		 ** @params: 	$msg   = Message to add to console output.
		 **						$state = true: Add message; false: clear msg.
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function AddExceptionMsg( $msg = "", $state = false ) {

			$this->ExceptionMsg = ( $state ) ? $msg : "";

		}



		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 **
		 **
		 ** @params: 	$n = Number of beeps.
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function Beep( $n = 1 ) {
			$beeps = "";

			for ( $i=0; $i < $n; $i++ ) $beeps .= "\x07";
			( (isset ($_SERVER['SERVER_PROTOCOL'])) ? false : print( $beeps) );

    }



		/** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** ** **
		 **
		 **
		 ** @params: 	Nothing
		 ** @return:	Nothing
		 ** ** ** ** **/
		public function QS_ETL_ODS() {

			// Query to extract call detail data from QSuite
			$QS_Qry = "
	SELECT
		1 AS client_id,
		cd.uuid,
		cd.phone_number,
		cd.callerid AS caller_id,
		cd.extradata AS extra_data,
		cd.startttime AS start_time,
		cd.connecttime AS connect_time,
		cd.endtime AS end_time,
		cd.duration,
		cd.fullduration AS full_duration,
		-- cd.camapign_id,
		-- cd.call_type_id,
		-- cd.roll_id,
		cd.subchannel,
		-- cd.call_list_id,
		-- cd.queue_id,
		cd.channel_name,
		cd.box_id,
		cd.trunk_name,
		-- cd.cause_code,
		cd.customer_id,
		cd.lastbridged AS last_bridged,
		cd.conf_id,
		-- cd.employee_id,
		cd.qpcdr_id,
		cd.sipcallid AS sip_call_id,
		cd.queuetime AS queue_time,
		q.queueid AS queue_id,
		q.queueName AS queue_name,
		call_types.id AS call_type_id,
		call_types.name AS call_type_name,
		call_types.value AS call_type_value,
		cc.cause_code,
		cc.name AS cause_code_name,
		-- cc.cause_type,
		cc.description AS cause_code_desc,
		cct.cause_code_type_id,
		cct.description AS cause_code_type_desc,
		rec.recording_id,
		-- rec.employee_id,
		rec.filename AS rec_filename,
		rec.length AS rec_length,
		rec.date AS rec_date,
		-- rec.call_history_id,
		-- rec.call_list_id,
		rec.dnis AS rec_dnis,
		rec.host AS rec_host,
		rec.admin_snoop AS rec_admin_snoop,
		rec.admin_exten AS rec_admin_exten,
		rec.uuid AS rec_uuid,
		rec.file_path AS rec_file_path,
		rec.deleted AS rec_deleted,
		rec.type_id AS rec_type_id,
		c.campaign_id,
		-- c.customerId,
		c.campName AS campaign_name,
		c.defaultScriptId default_script_id,
		c.campaign_type,
		-- c.call_center_id,
		c.cid_name,
		c.cid_number,
		c.pow_sorry_audiofile_id,
		c.url AS campaign_url,
		c.queueId AS campaign_queue_id,
		c.enable_call_list_search,
		c.dials,
		c.trunk_id,
		c.answer_timeout,
		c.force_wrapup_seconds,
		c.force_wrapup_termination,
		c.amd,
		c.amd_option,
		c.amd_initial_silence,
		c.amd_greeting,
		c.amd_after_greeting_silence,
		c.amd_total_analysis_time,
		c.amd_minimum_word_length,
		c.amd_between_words_silence,
		c.amd_maximum_number_of_words,
		c.amd_silence_threshold,
		c.qpump_sql_query_id,
		c.amd_audio_id,
		c.amd_dialplan_id,
		c.employee_owned_callbacks,
		c.show_reschedule,
		c.default_dial_mode,
		c.account_code_dialing,
		c.max_dial_count,
		c.expired_lead_days,
		c.max_call_count,
		c.force_wrapup_logout,
		c.active_flag,
		c.stopped,
		c.force_dial,
		c.record_transfers,
		c.allow_skipping_on_preview,
		c.hide_unowned_agent_callback_option,
		c.enable_ali_lookup,
		c.use_trunk_routing_rules,
		c.external_application_id,
		c.drop_call_dialplan_id,
		c.encryption_pwd,
		r.roll_id,
		-- r.campaign_id,
		r.date_of_working,
		r.seat_id,
		-- r.employee_id,
		r.login_type,
		r.web_server_id,
		r.ip_address,
		r.station_name,
		e.employee_id,
		e.first_name,
		e.last_name,
		e.employee_username AS emp_username,
		e.emp_passwd AS emp_password,
		e.street_address,
		e.city,
		e.province_state,
		e.postal_code,
		e.home_phone,
		e.other_phone,
		e.e_mail_id AS emp_email_id,
		e.call_center_id,
		-- e.customerId,
		e.privelege_id,
		e.can_login_as,
		e.team,
		e.emergency_name,
		e.emergency_phone1,
		e.emergency_phone2,
		e.rehire,
		e.enabled,
		e.security_level,
		e.personal_queue_id,
		e.agent_id,
		e.extension,
		e.aacbr,
		e.userportal_date_format,
		e.userportal_time_format,
		e.userportal_pagination,
		e.userportal_audiofile_format,
		e.pbx_reception_user,
		e.pbx_can_login_as,
		e.reschedule_callback_limit,
		e.ib_call_mode,
		e.enable_hot_desking,
		e.locale_id AS emp_locale_id,
		e.listen_id,
		e.default_login_inbound,
		e.default_login_outbound,
		e.agent_specified_login,
		e.ring_all_personal_queues,
		e.console_role_id,
		e.agent_allow_lead_creation,
		e.allow_agent_phone_login,
		e.agent_phone_login_ready,
		e.agent_phone_logout,
		e.agent_phone_login_pin,
		e.agent_phone_nailup_target,
		e.last_password_change_timestamp,
		e.department,
		cl.call_list_id,
		cl.callback_date_time,
		cl.call_date_time,
		-- cl.campaign_id,
		-- cl.employee_id,
		cl.phone_number AS cl_phone_number,
		cl.firstname AS cl_first_name,
		cl.lastname AS cl_last_name,
		cl.e_mail_id AS cl_email_id,
		cl.street,
		cl.st_or_province,
		cl.city AS cl_city,
		cl.postal_code AS cl_postal_code,
		cl.time_zone_id,
		cl.leadcode,
		cl.list_id,
		cl.priority,
		cl.last_termination,
		cl.date_inserted,
		cl.upload_id,
		cl.callerid_num AS cl_callerid_num,
		cl.callerid_name,
		-- cl.customerId,
		cl.account_code,
		cl.locale_id,
		cl.deleted,
		cl.prev_termination,
		cl.temp_call_list_id,
		cl.callable,
		cl.transfer_callerid_name,
		cl.transfer_callerid_num,
		cl.call_count,
		cl.dial_count,
		cl.max_dial_count AS cl_max_dial_count,
		cl.max_call_count AS cl_max_call_count,
		cl.rule_id,
		cl.phone_type_id,
		cl.phone_loaded_index,
		ch.call_history_id,
		ch.call_history_date_time,
		-- ch.call_termination_id,
		ch.duration AS ch_duration,
		-- ch.roll_id,
		-- ch.call_list_id,
		-- ch.campaign_id,
		ch.note,
		ch.callerid_num AS ch_callerid_num,
		ct.call_termination_id,
		ct.call_termination_type,
		-- ct.term_type_id,
		ct.default_term,
		ct.admin_viewable,
		ct.agent_owned,
		-- ct.customerId,
		ct.recyclable,
		ct.external_text,
		ct.external_value,
		ct.transfer_did_id,
		tt.term_type_id,
		tt.term_type_name,
		tt.admin_viewable AS tt_admin_viewable
	FROM qpcdr cd
	INNER JOIN call_types ON cd.call_type_id = call_types.id
	LEFT JOIN recordings rec ON cd.uuid = rec.uuid
	LEFT JOIN (
	call_history_qpcdr_link ch_link
	INNER JOIN call_history ch ON ch_link.call_history_id = ch.call_history_id
	LEFT JOIN (
		call_terminations ct
		INNER JOIN termination_types tt ON ct.term_type_id = tt.term_type_id
	) ON ch.call_termination_id = ct.call_termination_id
	) ON cd.uuid = ch_link.uuid
	LEFT JOIN campaign c ON cd.camapign_id = c.campaign_id
	LEFT JOIN (
	roll r
	INNER JOIN employee e ON r.employee_id = e.employee_id
	) ON cd.roll_id = r.roll_id
	LEFT JOIN call_list cl ON cd.call_list_id = cl.call_list_id
	LEFT JOIN queues q ON cd.queue_id = q.queueid
	LEFT JOIN (
	isdn_cause_code cc
	INNER JOIN isdn_cause_code_type cct ON cc.cause_type = cct.cause_code_type_id
	) ON cd.cause_code = cc.cause_code
	WHERE cd.endtime IS NOT NULL
		AND cd.endtime >= '" . $this->LastEndtime . "'
	ORDER BY cd.endtime ASC
	LIMIT 1;
	";

			// Query to load call detail data into ODS
			$ODS_Qry = "
			  INSERT INTO call_details (client_id, uuid, phone_number, caller_id, extra_data, start_time, connect_time, end_time, duration, full_duration, subchannel, channel_name, box_id, trunk_name, customer_id, last_bridged, conf_id, qpcdr_id, sip_call_id, queue_time, queue_id, queue_name, call_type_id, call_type_name, call_type_value, cause_code, cause_code_name, cause_code_desc, cause_code_type_id, cause_code_type_desc, recording_id, rec_filename, rec_length, rec_date, rec_dnis, rec_host, rec_admin_snoop, rec_admin_exten, rec_uuid, rec_file_path, rec_deleted, rec_type_id, campaign_id, campaign_name, default_script_id, campaign_type, cid_name, cid_number, pow_sorry_audiofile_id, campaign_url, campaign_queue_id, enable_call_list_search, dials, trunk_id, answer_timeout, force_wrapup_seconds, force_wrapup_termination, amd, amd_option, amd_initial_silence, amd_greeting, amd_after_greeting_silence, amd_total_analysis_time, amd_minimum_word_length, amd_between_words_silence, amd_maximum_number_of_words, amd_silence_threshold, qpump_sql_query_id, amd_audio_id, amd_dialplan_id, employee_owned_callbacks, show_reschedule, default_dial_mode, account_code_dialing, max_dial_count, expired_lead_days, max_call_count, force_wrapup_logout, active_flag, stopped, force_dial, record_transfers, allow_skipping_on_preview, hide_unowned_agent_callback_option, enable_ali_lookup, use_trunk_routing_rules, external_application_id, drop_call_dialplan_id, encryption_pwd, roll_id, date_of_working, seat_id, login_type, web_server_id, ip_address, station_name, employee_id, first_name, last_name, emp_username, emp_password, street_address, city, province_state, postal_code, home_phone, other_phone, emp_email_id, call_center_id, privelege_id, can_login_as, team, emergency_name, emergency_phone1, emergency_phone2, rehire, enabled, security_level, personal_queue_id, agent_id, extension, aacbr, userportal_date_format, userportal_time_format, userportal_pagination, userportal_audiofile_format, pbx_reception_user, pbx_can_login_as, reschedule_callback_limit, ib_call_mode, enable_hot_desking, emp_locale_id, listen_id, default_login_inbound, default_login_outbound, agent_specified_login, ring_all_personal_queues, console_role_id, agent_allow_lead_creation, allow_agent_phone_login, agent_phone_login_ready, agent_phone_logout, agent_phone_login_pin, agent_phone_nailup_target, last_password_change_timestamp, department, call_list_id, callback_date_time, call_date_time, cl_phone_number, cl_first_name, cl_last_name, cl_email_id, street, st_or_province, cl_city, cl_postal_code, time_zone_id, leadcode, list_id, priority, last_termination, date_inserted, upload_id, cl_callerid_num, callerid_name, account_code, locale_id, deleted, prev_termination, temp_call_list_id, callable, transfer_callerid_name, transfer_callerid_num, call_count, dial_count, cl_max_dial_count, cl_max_call_count, rule_id, phone_type_id, phone_loaded_index, call_history_id, call_history_date_time, ch_duration, note, ch_callerid_num, call_termination_id, call_termination_type, default_term, admin_viewable, agent_owned, recyclable, external_text, external_value, transfer_did_id, term_type_id, term_type_name, tt_admin_viewable)
			  VALUES (  ";


			try {

				$this->QSuite->setDataSource('qsuite');
				$this->QSuite->setSource('admin_page');

				// Fetch call details from QSuite
				$QS_Data = $this->QSuite->query( $QS_Qry );

				$this->LastEndtime = $QS_Data[0]['cd']['end_time'];

	  	} catch (Exception $ex) {

	    	$this->AddExceptionMsg( $ex->getMessage(), true );
	    	return;		// Unrecoverable

			}


			if ( $num = sizeof( $QS_Data ) ) {

				// Process each record & build sql
				foreach ( $QS_Data as $record ) {

					/* 0, 'cd', 'q', 'call_types', 'cc', 'cct', 'rec', 'c', 'r', 'e', 'cl', 'ch', 'ct', 'tt' */
					$ODS_Data = array_values( array_merge(
																		$record[0], $record['cd'], $record['q'], $record['call_types'], $record['cc'],
																		$record['cct'], $record['rec'], $record['c'], $record['r'], $record['e'],
																		$record['cl'], $record['ch'], $record['ct'], $record['tt']
																		) );

					// $ODS_Qry .= "   (";
					foreach ( $ODS_Data as $k => $v )	$ODS_Data[$k] = ( (empty($v)) ? "NULL" : ((is_numeric($v)) ? $v : "'".$v."'") );
					$ODS_Qry .= implode( ",", $ODS_Data );
					// $ODS_Qry .= "), ";
				}

				// $ODS_Qry  = substr( $ODS_Qry, 0, -2 );  		// Trim ending ","
				$ODS_Qry .= "  );";


				try {

					// Load call details into ODS
		    	$this->CallDetail->query( $ODS_Qry );

		  	} catch (Exception $ex) {

					// Check if it's a duplicate record exception
					if ( $ex->getCode() == self::SQLSTATE_23000 ) {
						// Yes, ignore it
					} else {
						// Nope, display it
		    		$this->AddExceptionMsg( $ex->getMessage(), true );
			    	return;		// Unrecoverable
			    }

				}

				$this->TotETLRecordsSent += ($this->NumETLRecordsSent = sizeof( $QS_Data ));

			}



			// Query to extract agent status data from QSuite
			$QS_Qry = "
	SELECT
		atp.timestamp,
		atp.duration_usec,
		atp.call_history_id,
		atp.queue_id,
		atp.changed_by,
		atp.campaign_id,
		atp.did,
		ustates.ustate_id,
		ustates.ustate_name,
		ustates.customerId AS customer_id,
		ustates.logout,
		r.roll_id,
		r.campaign_id AS roll_campaign_id,
		r.date_of_working,
		r.seat_id,
		r.login_type,
		r.web_server_id,
		r.ip_address,
		r.station_name,
		e.employee_id,
		e.first_name,
		e.last_name,
		e.employee_username,
		e.street_address,
		e.city,
		e.province_state,
		e.postal_code,
		e.home_phone,
		e.other_phone,
		e.e_mail_id,
		e.call_center_id,
		e.customerId AS emp_customer_id,
		e.privelege_id,
		e.can_login_as,
		e.team,
		e.emergency_name,
		e.emergency_phone1,
		e.emergency_phone2,
		e.rehire,
		e.enabled,
		e.security_level,
		e.personal_queue_id,
		e.agent_id,
		e.extension,
		e.aacbr,
		e.userportal_date_format,
		e.userportal_time_format,
		e.userportal_pagination,
		e.userportal_audiofile_format,
		e.pbx_reception_user,
		e.pbx_can_login_as,
		e.reschedule_callback_limit,
		e.ib_call_mode,
		e.enable_hot_desking,
		e.locale_id,
		e.listen_id,
		e.default_login_inbound,
		e.default_login_outbound,
		e.agent_specified_login,
		e.ring_all_personal_queues,
		e.console_role_id,
		e.agent_allow_lead_creation,
		e.allow_agent_phone_login,
		e.agent_phone_login_ready,
		e.agent_phone_logout,
		e.agent_phone_login_pin,
		e.agent_phone_nailup_target,
		e.last_password_change_timestamp,
		e.department
	FROM agent_time_profile atp
	INNER JOIN ustates ON atp.ustate_id = ustates.ustate_id
	INNER JOIN roll r ON atp.roll_id = r.roll_id
	INNER JOIN employee e ON r.employee_id = e.employee_id
	LIMIT 1;
	";

			// Query to load agent status data into ODS
			$ODS_Qry = "
			  INSERT INTO agent_state_details (timestamp, duration_usec, call_history_id, queue_id, changed_by, campaign_id, did, ustate_id, ustate_name, customer_id, logout, roll_id, roll_campaign_id, date_of_working, seat_id, login_type, web_server_id, ip_address, station_name, employee_id, first_name, last_name, employee_username, street_address, city, province_state, postal_code, home_phone, other_phone, e_mail_id, call_center_id, emp_customer_id, privelege_id, can_login_as, team, emergency_name, emergency_phone1, emergency_phone2, rehire, enabled, security_level, personal_queue_id, agent_id, extension, aacbr, userportal_date_format, userportal_time_format, userportal_pagination, userportal_audiofile_format, pbx_reception_user, pbx_can_login_as, reschedule_callback_limit, ib_call_mode, enable_hot_desking, locale_id, listen_id, default_login_inbound, default_login_outbound, agent_specified_login, ring_all_personal_queues, console_role_id, agent_allow_lead_creation, allow_agent_phone_login, agent_phone_login_ready, agent_phone_logout, agent_phone_login_pin, agent_phone_nailup_target, last_password_change_timestamp, department)
				VALUES (  ";


			try {

				// Fetch agent status details from QSuite
				$QS_Data = $this->QSuite->query( $QS_Qry );

	  	} catch (Exception $ex) {

	    	$this->AddExceptionMsg( $ex->getMessage(), true );
	    	return;		// Recoverable, but no point in continuing

			}


			if ( sizeof( $QS_Data ) ) {
				$num += sizeof( $QS_Data );

				// Process each record & build sql
				foreach ( $QS_Data as $record ) {

					/* 'atp', 'ustates', 'r', 'e' */
					$ODS_Data = array_values( array_merge(
																		$record['atp'], $record['ustates'], $record['r'], $record['e']
																		) );

					// $ODS_Qry .= "   (";
					foreach ( $ODS_Data as $k => $v )	$ODS_Data[$k] = ( (empty($v)) ? "NULL" : ((is_numeric($v)) ? $v : "'".$v."'") );
					$ODS_Qry .= implode( ",", $ODS_Data );
					// $ODS_Qry .= "), ";
				}

				// $ODS_Qry  = substr( $ODS_Qry, 0, -2 );  		// Trim ending ","
				$ODS_Qry .= "  );";


				try {

					// Load agent status details into ODS
		    	$this->AgentStateDetail->query( $ODS_Qry );

		  	} catch (Exception $ex) {

		    	$this->AddExceptionMsg( $ex->getMessage(), true );
		    	// return;		// Recoverable

				}

				$this->NumETLRecordsSent += sizeof( $QS_Data );
				$this->TotETLRecordsSent += sizeof( $QS_Data );

			}

			return( $num );
		} // End Function QS_ETL_ODS



	}	// End Class DoMonetShell
?>
