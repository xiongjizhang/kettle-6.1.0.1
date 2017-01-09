/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;

/**
 * Tests for SlaveServer class
 * 
 * @author Pavel Sakun
 * @see SlaveServer
 */
public class SlaveServerTest {
  SlaveServer slaveServer;

  @BeforeClass
  public static void beforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  @Before
  public void init() throws IOException {
    HttpClient httpClient = spy( new HttpClient() );
    doReturn( 404 ).when( httpClient ).executeMethod( any( HttpMethod.class ) );

    slaveServer = spy( new SlaveServer() );
    doReturn( httpClient ).when( slaveServer ).getHttpClient();
    doReturn( "response_body" ).when( slaveServer ).getResponseBodyAsString( any( InputStream.class ) );
  }

  @Test( expected = KettleException.class )
  public void testExecService() throws Exception {
    doReturn( mock( GetMethod.class ) ).when( slaveServer ).buildExecuteServiceMethod( anyString(),
        anyMapOf( String.class, String.class ) );
    slaveServer.execService( "wrong_app_name" );
    fail( "Incorrect connection details had been used, but no exception was thrown" );
  }

  @Test( expected = KettleException.class )
  public void testSendXML() throws Exception {
    doReturn( mock( PostMethod.class ) ).when( slaveServer ).buildSendXMLMethod( any( byte[].class ), anyString() );
    slaveServer.sendXML( "", "" );
    fail( "Incorrect connection details had been used, but no exception was thrown" );
  }

  @Test( expected = KettleException.class )
  public void testSendExport() throws Exception {
    doReturn( mock( PostMethod.class ) ).when( slaveServer ).buildSendExportMethod( anyString(), anyString(),
        any( InputStream.class ) );
    File tempFile;
    tempFile = File.createTempFile( "PDI-", "tmp" );
    tempFile.deleteOnExit();
    slaveServer.sendExport( tempFile.getAbsolutePath(), "", "" );
    fail( "Incorrect connection details had been used, but no exception was thrown" );
  }

  @Test
  public void testAddCredentials() {
    slaveServer.setUsername( "test_username" );
    slaveServer.setPassword( "test_password" );
    slaveServer.setHostname( "test_host" );
    slaveServer.setPort( "8081" );

    HttpClient client = slaveServer.getHttpClient();
    slaveServer.addCredentials( client );
    HttpClient authClient = slaveServer.getHttpClient();

    assertTrue( authClient.getParams().isAuthenticationPreemptive() );
    AuthScope scope = new AuthScope( slaveServer.getHostname(), Const.toInt( slaveServer.getPort(), 80 ) );
    Credentials credentials = authClient.getState().getCredentials( scope );
    assertNotNull( credentials );
    assertTrue( credentials instanceof UsernamePasswordCredentials );
    UsernamePasswordCredentials baseCredentials = (UsernamePasswordCredentials) credentials;
    assertEquals( slaveServer.getUsername(), baseCredentials.getUserName() );
    assertEquals( slaveServer.getPassword(), baseCredentials.getPassword() );
  }

  @Test
  public void testModifyingName() {
    slaveServer.setName( "test" );
    List<SlaveServer> list = new ArrayList<SlaveServer>();
    list.add( slaveServer );

    SlaveServer slaveServer2 = spy( new SlaveServer() );
    slaveServer2.setName( "test" );

    slaveServer2.verifyAndModifySlaveServerName( list, null );

    assertTrue( !slaveServer.getName().equals( slaveServer2.getName() ) );
  }
}
