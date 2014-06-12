<?php

  class AgentStateDetailController extends AppController {

  	// var $name = 'AgentStateDetail';

  	// var $scaffold;



    //******************************************//
    function beforeFilter() {
      // We don't want non-authenticated users accessing this controller.
      /// $this->checkSession();
      }



    //******************************************//
    function test_table( $id = null ) {
      $this->pageTitle = 'ODS =>ETL=> Monet Management';


      /// $this->set( 'ODS_Data', $this->AgentStatus->find( /*'all'*/ ) );


      // Debugger::dump( $this );  debug( $this );  exit();
      }



//******************************************//
    function test_query( $id = null ) {
      $this->pageTitle = 'ODS =>ETL=> Monet Management';



      // Debugger::dump( $this );  debug( $this );  exit();
      }



    //******************************************//
    function home( $id = null ) {
      $this->pageTitle = 'ODS =>ETL=> Monet Management';



      // Debugger::dump( $this );  debug( $this );  exit();
      }



}
?>