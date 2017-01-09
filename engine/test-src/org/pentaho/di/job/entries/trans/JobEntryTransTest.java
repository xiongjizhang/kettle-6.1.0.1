/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.job.entries.trans;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.Job;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class JobEntryTransTest {
  private final String JOB_ENTRY_TRANS_NAME = "JobEntryTransName";
  private final String JOB_ENTRY_FILE_NAME = "JobEntryFileName";
  private final String JOB_ENTRY_FILE_DIRECTORY = "JobEntryFileDirectory";
  private final String JOB_ENTRY_DESCRIPTION = "JobEntryDescription";

  //prepare xml for use
  public Node getEntryNode( boolean includeTransname, ObjectLocationSpecificationMethod method )
    throws ParserConfigurationException, SAXException, IOException {
    JobEntryTrans jobEntryTrans = getJobEntryTrans();
    jobEntryTrans.setDescription( JOB_ENTRY_DESCRIPTION );
    jobEntryTrans.setFileName( JOB_ENTRY_FILE_NAME );
    jobEntryTrans.setDirectory( JOB_ENTRY_FILE_DIRECTORY );
    if ( includeTransname ) {
      jobEntryTrans.setTransname( JOB_ENTRY_FILE_NAME );
    }
    if ( method != null ) {
      jobEntryTrans.setSpecificationMethod( method );
    }
    String string = "<job>" + jobEntryTrans.getXML() + "</job>";
    InputStream stream = new ByteArrayInputStream( string.getBytes( StandardCharsets.UTF_8 ) );
    DocumentBuilder db;
    Document doc;
    db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    doc = db.parse( stream );
    Node entryNode = doc.getFirstChild();
    return entryNode;
  }

  private JobEntryTrans getJobEntryTrans() {
    JobEntryTrans jobEntryTrans = new JobEntryTrans( JOB_ENTRY_TRANS_NAME );
    return jobEntryTrans;
  }

  @SuppressWarnings( "unchecked" )
  private void testJobEntry( Repository rep, boolean includeJobName, ObjectLocationSpecificationMethod method,
      ObjectLocationSpecificationMethod expectedMethod )
    throws KettleXMLException, ParserConfigurationException, SAXException, IOException {
    List<DatabaseMeta> databases = mock( List.class );
    List<SlaveServer> slaveServers = mock( List.class );
    IMetaStore metaStore = mock( IMetaStore.class );
    JobEntryTrans jobEntryTrans = getJobEntryTrans();
    jobEntryTrans.loadXML( getEntryNode( includeJobName, method ), databases, slaveServers, rep, metaStore );
    assertEquals( "If we connect to repository then we use rep_name method",
        expectedMethod, jobEntryTrans.getSpecificationMethod() );
  }

  /**
   * BACKLOG-179 - Exporting/Importing Jobs breaks Transformation specification when using "Specify by reference"
   * 
   * Test checks that we choose different {@link ObjectLocationSpecificationMethod} when connection to
   * {@link Repository} and disconnected. 
   * 
   * <b>Important!</b> You must rewrite test when change import logic
   * 
   * @throws KettleXMLException
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  @Test
  public void testChooseSpecMethodByRepositoryConnectionStatus()
    throws KettleXMLException, ParserConfigurationException, SAXException, IOException {
    Repository rep = mock( Repository.class );
    when( rep.isConnected() ).thenReturn( true );

    // 000
    // not connected, no jobname, no method
    testJobEntry( null, false, null, ObjectLocationSpecificationMethod.FILENAME );

    // 001
    // not connected, no jobname, REPOSITORY_BY_REFERENCE method
    testJobEntry( null, false, ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE, ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE );
    // not connected, no jobname, REPOSITORY_BY_NAME method
    testJobEntry( null, false, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );
    // not connected, no jobname, FILENAME method
    testJobEntry( null, false, ObjectLocationSpecificationMethod.FILENAME, ObjectLocationSpecificationMethod.FILENAME );

    // 010
    // not connected, jobname, no method
    testJobEntry( null, true, null, ObjectLocationSpecificationMethod.FILENAME );

    // 011
    // not connected, jobname, REPOSITORY_BY_REFERENCE method
    testJobEntry( null, true, ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE, ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE );
    // not connected, jobname, REPOSITORY_BY_NAME method
    testJobEntry( null, true, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );
    // not connected, jobname, FILENAME method
    testJobEntry( null, true, ObjectLocationSpecificationMethod.FILENAME, ObjectLocationSpecificationMethod.FILENAME );

    // 100
    // connected, no jobname, no method
    testJobEntry( rep, false, null, ObjectLocationSpecificationMethod.FILENAME );

    // 101
    // connected, no jobname, REPOSITORY_BY_REFERENCE method
    testJobEntry( rep, false, ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE, ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE );
    // connected, no jobname, REPOSITORY_BY_NAME method
    testJobEntry( rep, false, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );
    // connected, no jobname, FILENAME method
    testJobEntry( rep, false, ObjectLocationSpecificationMethod.FILENAME, ObjectLocationSpecificationMethod.FILENAME );

    // 110  
    // connected, jobname, no method
    testJobEntry( rep, true, null, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );

    // 111
    // connected, jobname, REPOSITORY_BY_REFERENCE method
    testJobEntry( rep, true, ObjectLocationSpecificationMethod.REPOSITORY_BY_REFERENCE, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );
    // connected, jobname, REPOSITORY_BY_NAME method
    testJobEntry( rep, true, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );
    // connected, jobname, FILENAME method    
    testJobEntry( rep, true, ObjectLocationSpecificationMethod.FILENAME, ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );
  }

  @Test
  public void testExecute_result_false_get_transMeta_exception() throws KettleException {
    JobEntryTrans jobEntryTrans = spy( new JobEntryTrans( JOB_ENTRY_TRANS_NAME ) );
    jobEntryTrans.setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );
    jobEntryTrans.setParentJob( mock( Job.class ) );
    jobEntryTrans.setLogLevel( LogLevel.NOTHING );
    doThrow( new KettleException( "Error while loading transformation" ) ).when( jobEntryTrans ).getTransMeta( any(
        Repository.class ), any( IMetaStore.class ), any( VariableSpace.class ) );
    Result result = mock( Result.class );

    jobEntryTrans.execute( result, 1 );
    verify( result ).setResult( false );
  }

}
