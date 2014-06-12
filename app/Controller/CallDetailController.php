<?php

  class CallDetailController extends AppController {

  	// var $name = 'CallDetail';

  	// var $scaffold;



    //******************************************//
    function beforeFilter() {
      // We don't want non-authenticated users accessing this controller.
      /// $this->checkSession();
      }



    //******************************************//
    function test_table( $id = null ) {
      $this->pageTitle = 'ODS =>ETL=> Monet Management';


      $this->set( 'ODS_Data', $this->CallDetail->find( /*'all'*/ ) );


      // Debugger::dump( $this );  debug( $this );  exit();
      }



//******************************************//
    function test_query( $id = null ) {
      $this->pageTitle = 'ODS =>ETL=> Monet Management';


			$sqry  = "SELECT";
			$sqry .= "	   campName AS Queue_ID,";
			$sqry .= "	   DATE(call_history_date_time) AS Call_Date,";
			$sqry .= "	   startttime AS Interval_Start,";
			$sqry .= "	   endtime AS Interval_End,";
			$sqry .= "	   @Status := ROUND(RAND()) AS Status,";													/* xXx */
			$sqry .= "	   CASE WHEN @Status THEN 1 ELSE 0 END AS Call_Handled,";					/* xXx */
			$sqry .= "	   CASE WHEN @Status THEN 0 ELSE 1 END AS Call_Abandoned,";				/* xXx */
			$sqry .= "	   CASE WHEN @Status THEN duration1 ELSE NULL END AS Length_Not_Dropped,";			/* xXx */
			$sqry .= "	   CASE WHEN @Status THEN NULL ELSE duration1 END AS Length_Call_Dropped,";			/* xXx */
			$sqry .= "	   ROUND(RAND()*100) AS Queue_Time,";															/* xXx */
			$sqry .= "	   ROUND(RAND()*100) AS acw,";																		/* xXx */
			$sqry .= "	   ROUND(RAND()) AS Service_Level";																/* xXx */
			$sqry .= "  FROM cxp_data_warehouse.call_details";
			$sqry .= "   ,(SELECT @Status := ROUND(RAND())) vars";
			$sqry .= "  WHERE";
			$sqry .= "  	    campName IS NOT NULL";
			$sqry .= "  	AND campName NOT LIKE '%sample%'";


			$mqry  = "SELECT";
			$mqry .= "    Queue_ID AS QueueID,";
			$mqry .= "    FLOOR(Interval_Start) AS IntervalStart,";
			$mqry .= "    CEILING(Interval_End) AS IntervalEnd,";
			$mqry .= "    SUM(Call_Handled) AS HandledCalls,";
			$mqry .= "    SUM(Call_Abandoned) AS AbandonedCalls,";
			$mqry .= "    IFNULL(ROUND(AVG(Length_Not_Dropped),0),0) AS ATT,";
			$mqry .= "    IFNULL(FLOOR(AVG(acw)),0) AS ACW,";
			$mqry .= "    IFNULL(ROUND(AVG(Queue_Time),0),0) AS ASA,";
			$mqry .= "    IFNULL(ROUND(AVG(Length_Call_Dropped),0),0) AS ATAB,";
			$mqry .= "    FLOOR(SUM(Service_Level) / COUNT(*) * 100) AS ServiceLevel,";
			$mqry .= "    ROUND(RAND()*100) AS Overflow,";															/* xXx */
			$mqry .= "    IFNULL(ROUND(AVG(Queue_Time),0),0) AS OverflowDelay,";				/* xXx */
			$mqry .= "    ROUND(RAND()*100) AS Staffed";																/* xXx */
			$mqry .= "  FROM (";
			$mqry .= 			$sqry;
			$mqry .= "       ) Call_Details";
			$mqry .= "  GROUP BY QueueID, Call_Date";
			$mqry .= "  ORDER BY QueueID, Call_Date";


      $this->set( 'ODS_Data', $this->CallDetail->query( $mqry ) );


      // Debugger::dump( $this );  debug( $this );  exit();
      }



    //******************************************//
    function home( $id = null ) {
      $this->pageTitle = 'ODS =>ETL=> Monet Management';
      // $this->ODS->ID = $id;



/*
      $conditions = (($this->Session->read( 'User.Type' ) == 'Agent') ? array( 'Form.Visible = 1' ) : null );

      $this->set( 'action', (($id == null) ? 'Add' : 'Edit') );
      $this->set( 'forms', $this->Form->find( 'all', array( 'conditions' => $conditions, 'order' => 'Form.Form_Type_ID, Form.Title', 'recursive' => 1 ) ) );

      $form_types = array( '' => '-- Select Category --' );
      $formtypes = $this->Form->FormType->find( 'all', array( 'conditions' => array( 'FormType.Visible = 1' ), 'order' => 'FormType.ID' ) );
      foreach ( $formtypes as $formtype ) $form_types[ $formtype['FormType']['ID'] ] = $formtype['FormType']['Text'];
      $this->set( 'form_types', $form_types );

      if ( empty($this->data) ) {
        if ( $id != null ) { $this->data = $this->Form->read( null, $id ); }
        }
      else {

        }
*/


      // Debugger::dump( $this );  debug( $this );  exit();
      }



}
?>