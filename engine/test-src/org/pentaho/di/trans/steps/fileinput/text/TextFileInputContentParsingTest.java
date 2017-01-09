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

package org.pentaho.di.trans.steps.fileinput.text;

import java.net.URL;

import org.junit.Test;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.steps.fileinput.BaseFileInputField;

public class TextFileInputContentParsingTest extends BaseTextParsingTest {

  @Test
  public void testDefaultOptions() throws Exception {

    initByFile( "default.csv" );

    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "first", "1", "1.1" }, { "second", "2", "2.2" }, { "third", "3", "3.3" } } );
  }

  @Test
  public void testSeparator() throws Exception {

    meta.content.separator = ",";
    initByFile( "separator.csv" );

    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "first", "1", "1.1" }, { "second", "2", "2.2" }, { "third;third", "3", "3.3" } } );
  }

  @Test
  public void testEscape() throws Exception {

    meta.content.escapeCharacter = "\\";
    initByFile( "escape.csv" );

    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "first", "1", "1.1" }, { "second", "2", "2.2" }, { "third;third", "3", "3.3" } } );
  }

  @Test
  public void testHeader() throws Exception {

    meta.content.header = false;
    initByFile( "default.csv" );

    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "Field 1", "Field 2", "Field 3" }, { "first", "1", "1.1" }, { "second", "2", "2.2" }, {
      "third", "3", "3.3" } } );
  }

  @Test
  public void testGzipCompression() throws Exception {

    meta.content.fileCompression = "GZip";
    initByFile( "default.csv.gz" );

    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "first", "1", "1.1" }, { "second", "2", "2.2" }, { "third", "3", "3.3" } } );
  }

  @Test
  public void testVfsGzipCompression() throws Exception {

    meta.content.fileCompression = "None";
    String url = "gz:" + this.getClass().getResource( inPrefix + "default.csv.gz" );
    initByURL( url );

    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "first", "1", "1.1" }, { "second", "2", "2.2" }, { "third", "3", "3.3" } } );
  }

  @Test
  public void testVfsBzip2Compression() throws Exception {

    meta.content.fileCompression = "None";
    String url = "bz2:" + this.getClass().getResource( inPrefix + "default.csv.bz2" );
    initByURL( url );

    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "first", "1", "1.1" }, { "second", "2", "2.2" }, { "third", "3", "3.3" } } );
  }

  @Test
  public void testFixedWidth() throws Exception {

    meta.content.fileType = "Fixed";
    initByFile( "fixed.csv" );

    setFields( new BaseFileInputField( "f1", 0, 7 ), new BaseFileInputField( "f2", 8, 7 ), new BaseFileInputField( "f3",
        16, 7 ) );

    process();

    check( new Object[][] { { "first  ", "1      ", "1.1" }, { "second ", "2      ", "2.2" }, { "third  ", "3      ",
      "3.3" } } );
  }

  @Test
  public void testFilterEmptyBacklog5381() throws Exception {

    meta.content.header = false;
    meta.content.fileType = "Fixed";
    meta.content.noEmptyLines = true;
    meta.content.fileFormat = "mixed";
    initByFile( "filterempty-BACKLOG-5381.csv" );

    setFields( new BaseFileInputField( "f", 0, 100 ) );

    process();

    check( new Object[][] { { "FirstLine => FirstLine " }, { "ThirdLine => SecondLine" }, { "SixthLine => ThirdLine" },
      { "NinthLine => FourthLine" }, { "" } } );
  }

  @Test
  public void testFilterVariables() throws Exception {

    initByFile( "default.csv" );

    Variables vars = new Variables();
    vars.setVariable( "VAR_TEST", "second" );
    data.filterProcessor =
        new TextFileFilterProcessor( new TextFileFilter[] { new TextFileFilter( 0, "${VAR_TEST}", false, false ) },
            vars );
    setFields( new BaseFileInputField(), new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "first", "1", "1.1" }, { "third", "3", "3.3" } } );
  }

  @Test
  public void testBOM_UTF8() throws Exception {

    meta.content.encoding = "UTF-32LE";
    meta.content.header = false;
    initByFile( "test-BOM-UTF-8.txt" );

    setFields( new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "data", "1" } } );
  }

  @Test
  public void testBOM_UTF16BE() throws Exception {

    meta.content.encoding = "UTF-32LE";
    meta.content.header = false;
    initByFile( "test-BOM-UTF-16BE.txt" );

    setFields( new BaseFileInputField(), new BaseFileInputField() );

    process();

    check( new Object[][] { { "data", "1" } } );
  }
}
