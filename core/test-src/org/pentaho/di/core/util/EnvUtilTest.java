/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.core.util;

import org.junit.Test;
import org.pentaho.di.core.Const;

import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Yury_Bakhmutski on 11/4/2015.
 */
public class EnvUtilTest {

  @Test
  public void testEnvironmentInit() throws Exception {
    EnvUtil.environmentInit();
    //See PDI-14522
    boolean expected = false;
    boolean actual = Boolean.valueOf( System.getProperties().get( Const.VFS_USER_DIR_IS_ROOT ).toString() );
    assertEquals( expected, actual );
  }

  @Test
  public void createLocale_Null() throws Exception {
    assertNull( EnvUtil.createLocale( null ) );
  }

  @Test
  public void createLocale_Empty() throws Exception {
    assertNull( EnvUtil.createLocale( "" ) );
  }

  @Test
  public void createLocale_SingleCode() throws Exception {
    assertEquals( Locale.ENGLISH, EnvUtil.createLocale( "en" ) );
  }

  @Test
  public void createLocale_DoubleCode() throws Exception {
    assertEquals( Locale.US, EnvUtil.createLocale( "en_US" ) );
  }
}
