/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
package org.pentaho.di.core.parameters;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.di.core.parameters.NamedParamsExceptionTest.assertMessage;

/**
 * Created by mburgess on 10/13/15.
 */
public class UnknownParamExceptionTest {

  UnknownParamException exception;

  @Test
  public void testConstructors() {
    exception = new UnknownParamException();
    assertNotNull( exception );
    assertMessage( "null", exception );

    exception = new UnknownParamException( "message" );
    assertMessage( "message", exception );

    Throwable t = mock( Throwable.class );
    when( t.getStackTrace() ).thenReturn( new StackTraceElement[0] );
    exception = new UnknownParamException( t );
    assertEquals( t, exception.getCause() );

    exception = new UnknownParamException( "message", t );
    assertMessage( "message", exception );
    assertEquals( t, exception.getCause() );
  }
}
