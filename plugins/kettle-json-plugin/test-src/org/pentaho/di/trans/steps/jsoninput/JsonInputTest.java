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

package org.pentaho.di.trans.steps.jsoninput;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import junit.framework.ComparisonFailure;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.LanguageChoice;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepErrorMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

public class JsonInputTest {

  protected static final String BASE_RAM_DIR = "ram:/jsonInputTest/";
  protected StepMockHelper<JsonInputMeta, JsonInputData> helper;

  protected static final String getBasicTestJson() {
    return "{ \"store\": {\n"
        + "    \"book\": [ \n"
        + "      { \"category\": \"reference\",\n"
        + "        \"author\": \"Nigel Rees\",\n"
        + "        \"title\": \"Sayings of the Century\",\n"
        + "        \"price\": 8.95\n"
        + "      },\n"
        + "      { \"category\": \"fiction\",\n"
        + "        \"author\": \"Evelyn Waugh\",\n"
        + "        \"title\": \"Sword of Honour\",\n"
        + "        \"price\": 12.99\n"
        + "      },\n"
        + "      { \"category\": \"fiction\",\n"
        + "        \"author\": \"Herman Melville\",\n"
        + "        \"title\": \"Moby Dick\",\n"
        + "        \"isbn\": \"0-553-21311-3\",\n"
        + "        \"price\": 8.99\n"
        + "      },\n"
        + "      { \"category\": \"fiction\",\n"
        + "        \"author\": \"J. R. R. Tolkien\",\n"
        + "        \"title\": \"The Lord of the Rings\",\n"
        + "        \"isbn\": \"0-395-19395-8\",\n"
        + "        \"price\": 22.99\n"
        + "      }\n"
        + "    ],\n"
        + "    \"bicycle\": {\n"
        + "      \"color\": \"red\",\n"
        + "      \"price\": 19.95\n"
        + "    }\n"
        + "  }\n"
        + "}";
  }

  @BeforeClass
  public static void init() throws KettleException {
    KettleClientEnvironment.init();
  }

  @Before
  public void setUp() {
    helper =
        new StepMockHelper<JsonInputMeta, JsonInputData>( "json input test", JsonInputMeta.class, JsonInputData.class );
    when( helper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
        helper.logChannelInterface );
    when( helper.trans.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() {
    helper.cleanUp();
  }

  @Test
  public void testAttrFilter() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[?(@.isbn)].author", new ValueMetaString( "author w/ isbn" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, "Herman Melville" },
                         new Object[] { jsonInputField, "J. R. R. Tolkien" } } );
  }

  @Test
  public void testChildDot() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$.store.bicycle.color", new ValueMetaString( "bcol" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, "red" } } );
    testSimpleJsonPath( "$.store.bicycle.price", new ValueMetaNumber( "p" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, 19.95 } } );
  }

  @Test
  public void testChildBrackets() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$.['store']['bicycle']['color']", new ValueMetaString( "bcol" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, "red" } } );
  }

  @Test
  public void testChildBracketsNDots() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$.['store'].['bicycle'].['color']", new ValueMetaString( "bcol" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, "red" } } );
  }

  @Test
  public void testIndex() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[2].title", new ValueMetaString( "title" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, "Moby Dick" } } );
  }

  @Test
  public void testIndexFirst() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[:2].category", new ValueMetaString( "category" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, "reference" },
                         new Object[] { jsonInputField, "fiction" } } );
  }

  @Test
  public void testIndexLastObj() throws Exception {
    final String jsonInputField = getBasicTestJson();
    JsonInput jsonInput =
        createBasicTestJsonInput( "$..book[-1:]", new ValueMetaString( "last book" ), "json",
            new Object[] { jsonInputField } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { jsonInputField,
                       "{ \"category\": \"fiction\",\n"
                     + "  \"author\": \"J. R. R. Tolkien\",\n"
                     + "  \"title\": \"The Lord of the Rings\",\n"
                     + "  \"isbn\": \"0-395-19395-8\",\n"
                     + "  \"price\": 22.99\n"
                     + "}\n" } );
    rowComparator.setComparator( 1, new JsonComparison() );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 2 );
    Assert.assertEquals( 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testIndexList() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[1,3].price", new ValueMetaNumber( "price" ),
        new Object[][] { new Object[] { jsonInputField } },
        new Object[][] { new Object[] { jsonInputField, 12.99 },
                         new Object[] { jsonInputField, 22.99 } } );
  }

  @Test
  public void testDualExp() throws Exception {
    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[?(@.isbn)].isbn" );
    isbn.setType( ValueMetaInterface.TYPE_STRING );
    JsonInputField price = new JsonInputField( "price" );
    price.setPath( "$..book[?(@.isbn)].price" );
    price.setType( ValueMetaInterface.TYPE_NUMBER );

    JsonInputMeta meta = createSimpleMeta( "json", isbn, price );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { null, "0-553-21311-3", 8.99 },
        new Object[] { null, "0-395-19395-8", 22.99 } );
    rowComparator.setComparator( 0, null );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 3 );
    Assert.assertEquals( "error", 0, jsonInput.getErrors() );
    Assert.assertEquals( "lines written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testDualExpMismatchError() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );
    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[?(@.isbn)].isbn" );
    isbn.setType( ValueMetaInterface.TYPE_STRING );
    JsonInputField price = new JsonInputField( "price" );
    price.setPath( "$..book[*].price" );
    price.setType( ValueMetaInterface.TYPE_NUMBER );

    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {
      JsonInputMeta meta = createSimpleMeta( "json", isbn, price );
      JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );

      processRows( jsonInput, 3 );

      Assert.assertEquals( "error", 1, jsonInput.getErrors() );
      Assert.assertEquals( "rows written", 0, jsonInput.getLinesWritten() );
      String errors = IOUtils.toString( new ByteArrayInputStream( out.toByteArray() ), StandardCharsets.UTF_8.name() );
      String expectedError =
          "The data structure is not the same inside the resource!"
          + " We found 4 values for json path [$..book[*].price],"
          + " which is different that the number returned for path [$..book[?(@.isbn)].isbn] (2 values)."
          + " We MUST have the same number of values for all paths.";
      Assert.assertTrue( "expected error", errors.contains( expectedError ) );
    }
  }

  @Test
  public void testBadExp() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {
      JsonInput jsonInput = createBasicTestJsonInput( "$..fail", new ValueMetaString( "result" ), "json",
          new Object[] { getBasicTestJson() } );
      processRows( jsonInput, 2 );
      Assert.assertEquals( "errors", 1, jsonInput.getErrors() );
      Assert.assertEquals( "rows written", 0, jsonInput.getLinesWritten() );

      String expectedError = "We can not find any data with path [$..fail]";
      String errors = IOUtils.toString( new ByteArrayInputStream( out.toByteArray() ), StandardCharsets.UTF_8.name() );
      Assert.assertTrue( "error", errors.contains( expectedError ) );
    }
  }

  @Test
  public void testRemoveSourceField() throws Exception {
    final String inCol = "json";
    JsonInputField jpath = new JsonInputField( "isbn" );
    jpath.setPath( "$..book[*].isbn" );
    jpath.setType( ValueMetaInterface.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( inCol, jpath );
    meta.setRemoveSourceField( true );
    meta.setIgnoreMissingPath( true );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { "0-553-21311-3" },
        new Object[] { "0-395-19395-8"} );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 3 );
    Assert.assertEquals( "errors", 0, jsonInput.getErrors() );
    Assert.assertEquals( "lines written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testSmallDoubles() throws Exception {
    // legacy parser handles these but positive exp would read null
    for ( String nbr : new String[] { "1e-20", "1.52999996e-20", "2.05E-20" } ) {
      final String ibgNbrInput =
          "{ \"number\": " + nbr + " }";
      testSimpleJsonPath( "$.number", new ValueMetaNumber( "not so big number" ),
          new Object[][] { new Object[] { ibgNbrInput } },
          new Object[][] { new Object[] { ibgNbrInput, Double.parseDouble( nbr ) } } );
    }
  }

  @Test
  public void testJgdArray() throws Exception {
    final String input =
        " { \"arr\": [ [ { \"a\": 1, \"b\": 1}, { \"a\": 1, \"b\": 2} ], [ {\"a\": 3, \"b\": 4 } ] ] }";
    JsonInput jsonInput =
        createBasicTestJsonInput( "$.arr", new ValueMetaString( "array" ), "json", new Object[] { input } );
    RowComparatorListener rowComparator =
        new RowComparatorListener(
            new Object[] { input, "[[{\"a\":1,\"b\":1},{\"a\":1,\"b\":2}],[{\"a\":3,\"b\":4}]]" } );
    rowComparator.setComparator( 1, new JsonComparison() );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 2 );
    Assert.assertEquals( 1, jsonInput.getLinesWritten() );
  }

  /**
   * if receiving rows, still echo if no results
   */
  @Test
  public void testNoPath() throws Exception {
    JsonInputField noPath = new JsonInputField( "price" );
    noPath.setPath( "$.path.to.nowhere" );
    noPath.setType( ValueMetaInterface.TYPE_STRING );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputMeta meta = createSimpleMeta( "json", noPath );
    final String input = getBasicTestJson();
    meta.setIgnoreMissingPath( true );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input }, new Object[] { input } );
    processRows( jsonInput, 2 );
    Assert.assertEquals( "output", 2, jsonInput.getLinesWritten() );
    Assert.assertEquals( "errors", 0, jsonInput.getErrors() );
  }


  @Test
  public void testBfsMatchOrder() throws Exception {
    // streaming will be dfs..ref impl is bfs
    String input = "{ \"a\": { \"a\" : { \"b\" :2 } , \"b\":1 } }";
    JsonInput jsonInput =
        createBasicTestJsonInput( "$..a.b", new ValueMetaInteger( "b" ), "in", new Object[] { input } );
    RowComparatorListener rowComparator = new RowComparatorListener( jsonInput,
        new Object[] { input, 1L },
        new Object[] { input, 2L } );
    rowComparator.setComparator( 0, null );
    processRows( jsonInput, 2 );
    Assert.assertEquals( 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testRepeatFieldSingleObj() throws Exception {
    final String input = " { \"items\": [ "
        + "{ \"a\": 1, \"b\": null }, "
        + "{ \"a\":null, \"b\":2 }, "
        + "{ \"a\":3, \"b\":null }, "
        + "{ \"a\":4, \"b\":4 } ] }";
    final String inCol = "input";

    JsonInputField aField = new JsonInputField();
    aField.setName( "a" );
    aField.setPath( "$.items[*].a" );
    aField.setType( ValueMetaInterface.TYPE_INTEGER );
    JsonInputField bField = new JsonInputField();
    bField.setName( "b" );
    bField.setPath( "$.items[*].b" );
    bField.setType( ValueMetaInterface.TYPE_INTEGER );
    bField.setRepeated( true );

    JsonInputMeta meta = createSimpleMeta( inCol, aField, bField );
    JsonInput step = createJsonInput( inCol, meta, new Object[] { input } );
    step.addRowListener(
        new RowComparatorListener(
            new Object[] { input, 1L, null },
            new Object[] { input, null, 2L },
            new Object[] { input, 3L, 2L },
            new Object[] { input, 4L, 4L } ) );
    processRows( step, 4 );
    Assert.assertEquals( 4, step.getLinesWritten() );
  }

  /**
   * PDI-10384 Huge numbers causing exception in JSON input step<br>
   */
  @Test
  public void testLargeDoubles() throws Exception {
    // legacy mode yields null for these
    for ( String nbr : new String[] { "1e20", "2.05E20", "1.52999996e20" } ) {
      final String ibgNbrInput =
          "{ \"number\": " + nbr + " }";
      testSimpleJsonPath( "$.number", new ValueMetaNumber( "not so big number" ),
          new Object[][] { new Object[] { ibgNbrInput } },
          new Object[][] { new Object[] { ibgNbrInput, Double.parseDouble( nbr ) } } );
    }
  }



  @Test
  public void testNullProp() throws Exception {
    final String input = "{ \"obj\": [ { \"nval\": null, \"val\": 2 }, { \"val\": 1 } ] }";
    JsonInput jsonInput =
        createBasicTestJsonInput( "$.obj[?(@.nval)].val", new ValueMetaString( "obj" ), "json", new Object[] { input } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { input, "2" } );
    rowComparator.setComparator( 1, new JsonComparison() );
    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, 2 );
    // in jsonpath 2.0->2.1, null value properties started being counted as existing
    Assert.assertEquals( 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testDualExpMismatchPathLeafToNull() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[*].isbn" );
    isbn.setType( ValueMetaInterface.TYPE_STRING );
    JsonInputField price = new JsonInputField( "price" );
    price.setPath( "$..book[*].price" );
    price.setType( ValueMetaInterface.TYPE_NUMBER );

    JsonInputMeta meta = createSimpleMeta( "json", isbn, price );
    meta.setRemoveSourceField( true );
    meta.setDefaultPathLeafToNull( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { null, 8.95d },
        new Object[] { null, 12.99d },
        new Object[] { "0-553-21311-3", 8.99d },
        new Object[] { "0-395-19395-8", 22.99d } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 5 );

    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 4, jsonInput.getLinesWritten() );
  }

  @Test
  public void testPdi7770PathCardinality() throws Exception {
    // started working with json-path 2.1
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    final String input = "{ \"results\": ["
        + "  { \"badger\": 1 },"
        + "  { \"badger\": 2 },"
        + "  { \"badger\": 3, \"mushroom\" : 1 }]}";

    JsonInputField badger = new JsonInputField( "badger" );
    badger.setPath( "$..badger" );
    badger.setType( ValueMetaInterface.TYPE_STRING );
    JsonInputField mushroom = new JsonInputField( "mushroom" );
    mushroom.setPath( "$..mushroom" );
    mushroom.setType( ValueMetaInterface.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", badger, mushroom );
    meta.setDefaultPathLeafToNull( true );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { null, null },
        new Object[] { 1L, null },
        new Object[] { 2L, null },
        new Object[] { 3L, 1L } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 5 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 4, jsonInput.getLinesWritten() );
  }

  @Test
  public void testPdi7770PathCardinalityWorkaround() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    final String input = "{ \"results\": ["
        + "  { \"badger\": 1 },"
        + "  { \"badger\": 2 },"
        + "  { \"badger\": 3, \"mushroom\" : 1 }]}";

    JsonInputField badger = new JsonInputField( "badger" );
    badger.setPath( "$..results[*].badger" );
    badger.setType( ValueMetaInterface.TYPE_INTEGER );
    JsonInputField mushroom = new JsonInputField( "mushroom" );
    mushroom.setPath( "$..results[*].mushroom" );
    mushroom.setType( ValueMetaInterface.TYPE_INTEGER );

    JsonInputMeta meta = createSimpleMeta( "json", badger, mushroom );
    meta.setDefaultPathLeafToNull( true );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { 1L, null },
        new Object[] { 2L, null },
        new Object[] { 3L, 1L } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 5 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 3, jsonInput.getLinesWritten() );
  }

  @Test
  public void testSingleObjPred() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField bic = new JsonInputField( "color" );
    bic.setPath( "$.store.bicycle[?(@.price)].color" );
    bic.setType( ValueMetaInterface.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", bic );
    meta.setDefaultPathLeafToNull( true );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { "red" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testArrayOut() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField byc = new JsonInputField( "books (array)" );
    byc.setPath( "$.store.book" );
    byc.setType( ValueMetaInterface.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", byc );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { "[{\"category\":\"reference\",\"author\":\"Nigel Rees\",\"title\":\"Sayings of the Century\",\"price\":8.95},{\"category\":\"fiction\",\"author\":\"Evelyn Waugh\",\"title\":\"Sword of Honour\",\"price\":12.99},{\"category\":\"fiction\",\"author\":\"Herman Melville\",\"title\":\"Moby Dick\",\"isbn\":\"0-553-21311-3\",\"price\":8.99},{\"category\":\"fiction\",\"author\":\"J. R. R. Tolkien\",\"title\":\"The Lord of the Rings\",\"isbn\":\"0-395-19395-8\",\"price\":22.99}]" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testObjectOut() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField bic = new JsonInputField( "the bicycle (obj)" );
    bic.setPath( "$.store.bicycle" );
    bic.setType( ValueMetaInterface.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", bic );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { "{\"color\":\"red\",\"price\":19.95}" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    Assert.assertEquals( out.toString(), 0, jsonInput.getErrors() );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  @Test
  public void testBicycleAsterisk() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );

    JsonInputField byc = new JsonInputField( "badger" );
    byc.setPath( "$.store.bicycle[*]" );
    byc.setType( ValueMetaInterface.TYPE_STRING );

    JsonInputMeta meta = createSimpleMeta( "json", byc );
    meta.setRemoveSourceField( true );

    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { getBasicTestJson() } );
    RowComparatorListener rowComparator = new RowComparatorListener(
        new Object[] { "red" },
        new Object[] { "19.95" } );
    jsonInput.addRowListener( rowComparator );

    processRows( jsonInput, 2 );
    assertEquals( out.toString(), 0, jsonInput.getErrors() );
    assertEquals( "rows written", 2, jsonInput.getLinesWritten() );
  }

  @Test
  public void testNullInputs() throws Exception {
    final String jsonInputField = getBasicTestJson();
    testSimpleJsonPath( "$..book[?(@.isbn)].author", new ValueMetaString( "author w/ isbn" ),
        new Object[][] {
          new Object[] { null },
          new Object[] { jsonInputField },
          new Object[] { null } },
        new Object[][] {
          new Object[] { jsonInputField, "Herman Melville" },
          new Object[] { jsonInputField, "J. R. R. Tolkien" } } );
  }

  /**
   * File tests
   */
  @Test
  public void testFileList() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    final String input1 = getBasicTestJson();
    final String input2 = "{ \"store\": { \"book\": [ { \"price\": 9.99 } ] } }";
    try ( FileObject fileObj1 = KettleVFS.getFileObject( BASE_RAM_DIR + "test1.json" );
        FileObject fileObj2 = KettleVFS.getFileObject( BASE_RAM_DIR + "test2.json" ) ) {
      try ( OutputStream out = fileObj1.getContent().getOutputStream() ) {
        out.write( input1.getBytes() );
      }
      try ( OutputStream out = fileObj2.getContent().getOutputStream() ) {
        out.write( input2.getBytes() );
      }
      JsonInputField price = new JsonInputField();
      price.setName( "price" );
      price.setType( ValueMetaInterface.TYPE_NUMBER );
      price.setPath( "$..book[*].price" );
      List<FileObject> fileList = Arrays.asList( fileObj1, fileObj2 );
      JsonInputMeta meta = createFileListMeta( fileList );
      meta.setInputFields( new JsonInputField[] { price } );

      meta.setIncludeRowNumber( true );
      meta.setRowNumberField( "rownbr" );

      meta.setShortFileNameField( "fname" );

      JsonInput jsonInput = createJsonInput( meta );
      RowComparatorListener rowComparator = new RowComparatorListener(
          new Object[] { 8.95d, 1L, "test1.json" },
          new Object[] { 12.99d, 2L, "test1.json" },
          new Object[] { 8.99d, 3L, "test1.json" },
          new Object[] { 22.99d, 4L, "test1.json" },
          new Object[] { 9.99d, 5L, "test2.json" } );
      jsonInput.addRowListener( rowComparator );

      processRows( jsonInput, 5 );
      assertEquals( err.toString(), 0, jsonInput.getErrors() );
    } finally {
      deleteFiles();
    }
  }


  @Test
  public void testNoFilesInListError() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    JsonInputMeta meta = createFileListMeta( Collections.<FileObject>emptyList() );
    meta.setDoNotFailIfNoFile( false );
    JsonInputField price = new JsonInputField();
    price.setName( "price" );
    price.setType( ValueMetaInterface.TYPE_NUMBER );
    price.setPath( "$..book[*].price" );
    meta.setInputFields( new JsonInputField[] { price } );

    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {
      JsonInput jsonInput = createJsonInput( meta );
      processRows( jsonInput, 1 );
    }
    String errMsgs = err.toString();
    assertTrue( errMsgs, errMsgs.contains( "No file(s) specified!" ) );
  }

  @Test
  public void testZipFileInput() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    final String input = getBasicTestJson();
    try ( FileObject fileObj = KettleVFS.getFileObject( BASE_RAM_DIR + "test.zip" ) ) {
      fileObj.createFile();
      try ( OutputStream out = fileObj.getContent().getOutputStream() ) {
        try ( ZipOutputStream zipOut = new ZipOutputStream( out ) ) {
          ZipEntry jsonFile = new ZipEntry( "test.json" );
          zipOut.putNextEntry( jsonFile );
          zipOut.write( input.getBytes() );
          zipOut.closeEntry();
          zipOut.flush();
        }
      }
      JsonInputField price = new JsonInputField();
      price.setName( "price" );
      price.setType( ValueMetaInterface.TYPE_NUMBER );
      price.setPath( "$..book[*].price" );

      JsonInputMeta meta = createSimpleMeta( "in file", price );
      meta.setIsAFile( true );
      meta.setRemoveSourceField( true );
      JsonInput jsonInput = createJsonInput( "in file", meta, new Object[][] {
        new Object[] { "zip:" + BASE_RAM_DIR + "test.zip!/test.json" }
      } );
      RowComparatorListener rowComparator = new RowComparatorListener(
          new Object[] { 8.95d },
          new Object[] { 12.99d },
          new Object[] { 8.99d },
          new Object[] { 22.99d } );
      jsonInput.addRowListener( rowComparator );
      processRows( jsonInput, 5 );
      Assert.assertEquals( err.toString(), 0, jsonInput.getErrors() );
    } finally {
      deleteFiles();
    }
  }

  @Test
  public void testExtraFileFields() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog( err, LogLevel.ERROR );

    final String input1 = getBasicTestJson();
    final String input2 = "{ \"store\": { \"bicycle\": { \"color\": \"blue\" } } }";
    final String path1 = BASE_RAM_DIR + "test1.json";
    final String path2 = BASE_RAM_DIR + "test2.js";
    try ( FileObject fileObj1 = KettleVFS.getFileObject( path1 );
        FileObject fileObj2 = KettleVFS.getFileObject( path2 ) ) {
      try ( OutputStream out = fileObj1.getContent().getOutputStream() ) {
        out.write( input1.getBytes() );
      }
      try ( OutputStream out = fileObj2.getContent().getOutputStream() ) {
        out.write( input2.getBytes() );
      }

      JsonInputField color = new JsonInputField();
      color.setName( "color" );
      color.setType( ValueMetaInterface.TYPE_STRING );
      color.setPath( "$.store.bicycle.color" );

      JsonInputMeta meta = createSimpleMeta( "in file", color );
      meta.setInFields( true );
      meta.setIsAFile( true );
      meta.setRemoveSourceField( true );

      meta.setExtensionField( "extension" );
      meta.setPathField( "dir path" );
      meta.setSizeField( "size" );
      meta.setIsHiddenField( "hidden?" );
      meta.setLastModificationDateField( "last modified" );
      meta.setUriField( "URI" );
      meta.setRootUriField( "root URI" );

      // custom checkers for size and last modified
      RowComparatorListener rowComparator = new RowComparatorListener(
          new Object[] { "red",
            "json", "ram:///jsonInputTest", -1L, false, new Date( 0 ), "ram:///jsonInputTest/test1.json", "ram:///" },
          new Object[] { "blue",
            "js", "ram:///jsonInputTest", -1L, false, new Date( 0 ), "ram:///jsonInputTest/test2.js", "ram:///"} );
      rowComparator.setComparator( 3, new RowComparatorListener.Comparison<Object>() {
        @Override
        public boolean equals( Object expected, Object actual ) throws Exception {
          // just want a valid size
          return ( (long) actual ) > 0L;
        }
      } );
      rowComparator.setComparator( 5, new RowComparatorListener.Comparison<Object>() {
        @Override
        public boolean equals( Object expected, Object actual ) throws Exception {
          return ( (Date) actual ).after( new Date( 0 ) );
        }
      } );
      JsonInput jsonInput = createJsonInput( "in file", meta, new Object[][] {
        new Object[] { path1 },
        new Object[] { path2 }
      } );
      jsonInput.addRowListener( rowComparator );
      processRows( jsonInput, 3 );
      Assert.assertEquals( err.toString(), 0, jsonInput.getErrors() );
    } finally {
      deleteFiles();
    }
  }

  @Test
  public void testZeroSizeFile() throws Exception {
    ByteArrayOutputStream log = new ByteArrayOutputStream();
    helper.redirectLog( log, LogLevel.BASIC );
    try ( FileObject fileObj = KettleVFS.getFileObject( BASE_RAM_DIR + "test.json" );
          LocaleChange enUs = new LocaleChange( Locale.US ); ) {
      fileObj.createFile();
      JsonInputField price = new JsonInputField();
      price.setName( "price" );
      price.setType( ValueMetaInterface.TYPE_NUMBER );
      price.setPath( "$..book[*].price" );

      JsonInputMeta meta = createSimpleMeta( "in file", price );
      meta.setIsAFile( true );
      meta.setRemoveSourceField( true );
      meta.setIgnoreEmptyFile( false );
      JsonInput jsonInput = createJsonInput( "in file", meta, new Object[][] {
        new Object[] { BASE_RAM_DIR + "test.json" }
      } );
      processRows( jsonInput, 1 );
      String logMsgs = log.toString();
      assertTrue( logMsgs, logMsgs.contains( "is empty!" ) );
    } finally {
      deleteFiles();
    }
  }

  /**
   * PDI-13859
   */
  @Test
  public void testBracketEscape() throws Exception {
    String input = "{\"a\":1,\"b(1)\":2}";
    testSimpleJsonPath( "$.['b(1)']", new ValueMetaInteger( "b(1)" ),
        new Object[][] { new Object[] { input } },
        new Object[][] { new Object[] { input, 2L } } );
  }


  @Test
  public void testBadInput() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    helper.redirectLog( out, LogLevel.ERROR );
    JsonInputField isbn = new JsonInputField( "isbn" );
    isbn.setPath( "$..book[?(@.isbn)].isbn" );
    isbn.setType( ValueMetaInterface.TYPE_STRING );

    String input = "{{";
    try ( LocaleChange enUS = new LocaleChange( Locale.US ) ) {
      JsonInputMeta meta = createSimpleMeta( "json", isbn );
      JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input } );
      processRows( jsonInput, 3 );

      Assert.assertEquals( "error", 1, jsonInput.getErrors() );
      Assert.assertEquals( "rows written", 0, jsonInput.getLinesWritten() );
      String errors = IOUtils.toString( new ByteArrayInputStream( out.toByteArray() ), StandardCharsets.UTF_8.name() );
      Assert.assertTrue( "expected error", errors.contains( "Error parsing string" ) );
    }
  }

  @Test
  public void testErrorRedirect() throws Exception {
    JsonInputField field = new JsonInputField( "value" );
    field.setPath( "$.value" );
    field.setType( ValueMetaInterface.TYPE_STRING );

    String input1 = "{{";
    String input2 = "{ \"value\": \"ok\" }";

    JsonInputMeta meta = createSimpleMeta( "json", field );
    meta.setRemoveSourceField( true );
    when( helper.stepMeta.isDoingErrorHandling() ).thenReturn( true );
    JsonInput jsonInput = createJsonInput( "json", meta, new Object[] { input1 }, new Object[] { input2 } );
    StepErrorMeta errMeta = new StepErrorMeta( jsonInput, helper.stepMeta );
    errMeta.setEnabled( true );
    errMeta.setErrorFieldsValuename( "err field" );
    when( helper.stepMeta.getStepErrorMeta() ).thenReturn( errMeta );
    final List<Object[]> errorLines = new ArrayList<>();
    jsonInput.addRowListener( new RowComparatorListener( new Object[] { "ok" } ) {
      @Override
      public void errorRowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
        errorLines.add( row );
      }
    } );
    processRows( jsonInput, 3 );
    Assert.assertEquals( "fwd error", 1, errorLines.size() );
    Assert.assertEquals( "input in err line", input1, errorLines.get( 0 )[0] );
    Assert.assertEquals( "rows written", 1, jsonInput.getLinesWritten() );
  }

  protected JsonInputMeta createSimpleMeta( String inputColumn, JsonInputField ... jsonPathFields ) {
    JsonInputMeta jsonInputMeta = new JsonInputMeta();
    jsonInputMeta.setDefault();
    jsonInputMeta.setInFields( true );
    jsonInputMeta.setFieldValue( inputColumn );
    jsonInputMeta.setInputFields( jsonPathFields );
    jsonInputMeta.setIgnoreMissingPath( false );
    return jsonInputMeta;
  }

  private void deleteFiles() throws FileSystemException, KettleFileException {
    try ( FileObject baseDir = KettleVFS.getFileObject( BASE_RAM_DIR ) ) {
      baseDir.deleteAll();
    }
  }

  protected JsonInput createJsonInput( JsonInputMeta meta ) {
    JsonInputData data = new JsonInputData();

    JsonInput jsonInput = new JsonInput( helper.stepMeta, helper.stepDataInterface, 0, helper.transMeta, helper.trans );
    jsonInput.init( meta, data );
    return jsonInput;
  }

  protected JsonInputMeta createFileListMeta( final List<FileObject> files ) {
    JsonInputMeta meta = new JsonInputMeta() {
      @Override
      public FileInputList getFileInputList( VariableSpace space ) {
        return new FileInputList() {
          @Override
          public List<FileObject> getFiles() {
            return files;
          }
          @Override
          public int nrOfFiles() {
            return files.size();
          }
        };
      }
    };
    meta.setDefault();
    meta.setInFields( false );
    meta.setIgnoreMissingPath( false );
    return meta;
  }

  protected void testSimpleJsonPath( String jsonPath,
      ValueMetaInterface outputMeta,
      Object[][] inputRows, Object[][] outputRows ) throws Exception {
    final String inCol = "in";

    JsonInput jsonInput = createBasicTestJsonInput( jsonPath, outputMeta, inCol, inputRows );

    RowComparatorListener rowComparator = new RowComparatorListener( outputRows );

    jsonInput.addRowListener( rowComparator );
    processRows( jsonInput, outputRows.length + 1 );
    Assert.assertEquals( "rows written", outputRows.length, jsonInput.getLinesWritten() );
    Assert.assertEquals( "errors", 0, jsonInput.getErrors() );
  }

  protected void processRows( StepInterface step, final int maxCalls ) throws Exception {
    for ( int outRowIdx = 0; outRowIdx < maxCalls; outRowIdx++ ) {
      if ( !step.processRow( helper.processRowsStepMetaInterface, helper.processRowsStepDataInterface ) ) {
        break;
      }
    }
  }

  protected JsonInput createBasicTestJsonInput( String jsonPath, ValueMetaInterface outputMeta, final String inCol,
      Object[]... inputRows ) {
    JsonInputField jpath = new JsonInputField( outputMeta.getName() );
    jpath.setPath( jsonPath );
    jpath.setType( outputMeta.getType() );

    JsonInputMeta meta = createSimpleMeta( inCol, jpath );
    return createJsonInput( inCol, meta, inputRows );
  }

  protected JsonInput createJsonInput( final String inCol, JsonInputMeta meta, Object[]... inputRows ) {
    JsonInputData data = new JsonInputData();

    JsonInput jsonInput = new JsonInput( helper.stepMeta, helper.stepDataInterface, 0, helper.transMeta, helper.trans );

    RowSet input = helper.getMockInputRowSet( inputRows );
    RowMetaInterface rowMeta = createRowMeta( new ValueMetaString( inCol ) );
    input.setRowMeta( rowMeta );
    jsonInput.getInputRowSets().add( input );
    jsonInput.setInputRowMeta( rowMeta );
    jsonInput.init( meta, data );
    return jsonInput;
  }

  protected static class RowComparatorListener extends RowAdapter {

    Object[][] data;
    int rowNbr = 0;
    private Map<Integer, Comparison<Object>> comparators = new HashMap<>();

    public RowComparatorListener( Object[]... data ) {
      this.data = data;
    }

    public RowComparatorListener( StepInterface step, Object[]... data ) {
      this.data = data;
      step.addRowListener( this );
    }

    /**
     * @param colIdx
     * @param comparator
     */
    public void setComparator( int colIdx, Comparison<Object> comparator ) {
      comparators.put( colIdx, comparator );
    }

    @Override
    public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
      if ( rowNbr >= data.length ) {
        throw new ComparisonFailure( "too many output rows", "" + data.length, "" + rowNbr + 1 );
      } else {
        for ( int i = 0; i < data[rowNbr].length; i++ ) {
          try {
            boolean eq = true;
            if ( comparators.containsKey( i ) ) {
              Comparison<Object> comp = comparators.get( i );
              if ( comp != null ) {
                eq = comp.equals( data[rowNbr][i], row[i] );
              }
            } else {
              ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
              eq = valueMeta.compare( data[rowNbr][i], row[i] ) == 0;
            }
            if ( !eq ) {
              throw new ComparisonFailure( String.format( "Mismatch row %d, column %d", rowNbr, i ), rowMeta
                  .getString( data[rowNbr] ), rowMeta.getString( row ) );
            }
          } catch ( Exception e ) {
            throw new AssertionError( String.format( "Value type at row %d, column %d", rowNbr, i ), e );
          }
        }
        rowNbr++;
      }
    }

    protected static interface Comparison<T> {
      public boolean equals( T expected, T actual ) throws Exception;
    }
  }

  protected static class JsonComparison implements RowComparatorListener.Comparison<Object> {
    @Override
    public boolean equals( Object expected, Object actual ) throws Exception {
      return jsonEquals( (String) expected, (String) actual );
    }
  }

  protected static class LocaleChange implements AutoCloseable {

    private Locale original;

    public LocaleChange( Locale newLocale ) {
      original = LanguageChoice.getInstance().getDefaultLocale();
      LanguageChoice.getInstance().setDefaultLocale( newLocale );
    }

    @Override
    public void close() throws Exception {
      LanguageChoice.getInstance().setDefaultLocale( original );
    }
  }

  /**
   * compare json (deep equals ignoring order)
   */
  protected static final boolean jsonEquals( String json1, String json2 ) throws Exception {
    ObjectMapper om = new ObjectMapper();
    JsonNode parsedJson1 = om.readTree( json1 );
    JsonNode parsedJson2 = om.readTree( json2 );
    return parsedJson1.equals( parsedJson2 );
  }

  protected static RowMetaInterface createRowMeta( ValueMetaInterface ... valueMetas ) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList( Arrays.asList( valueMetas ) );
    return rowMeta;
  }
}
