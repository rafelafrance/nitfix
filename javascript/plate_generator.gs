var ROW_COUNT = 8;
var COL_COUNT = 12;
var DATE_OFFSET = 1; // Add this to get to the date cell
var LOCAL_ID_OFFSET = 2; // Add this to get to the date cell
var PROTOCOL_OFFSET = 3; // Add this to get to the protocol cell
var NOTES_OFFSET = 4; // Add this to get to the notes cell
var RESULTS_OFFSET = 5; // Add this to get to the results cell
var TABLE_OFFSET = 6; // Add this to get to the top of the sample table
var LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
var BACKGROUND = '#fdffa8';


function setup() {
  const sheet = SpreadsheetApp.getActiveSheet();

  const values = [
    []
  ];
  values[0].push('Plate ID');
  for (c = 0; c < COL_COUNT; c++) {
    values[0].push('Plate column ' + LETTERS[c]);
  }
  sheet.getRange(1, 1, 1, COL_COUNT + 1).setValues(values);

  sheet.setFrozenRows(1);
  sheet.setFrozenColumns(1);
}


function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Plates')
    .addItem('Add plate template', 'addTemplate')
    .addToUi();
}


function addTemplate() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const cell = sheet.getActiveCell();
  const plateId = cell.getDisplayValue();
  const row = cell.getRow();
  const col = cell.getColumn();

  Logger.log('Generating for plate: ' + plateId);

  validate(row, col, plateId);

  const today = ((new Date()).toISOString()).slice(0, 10);
  addRowText(row, col, DATE_OFFSET, today);
  addRowText(row, col, LOCAL_ID_OFFSET, 'Local identifier');
  addRowText(row, col, PROTOCOL_OFFSET, 'Protocol');
  addRowText(row, col, NOTES_OFFSET, 'Notes');
  addRowText(row, col, RESULTS_OFFSET, 'Link to quantification results');
  addRowLabels(row, col);
  addBody(row, col);
}


function addRowText(row, col, offset, text) {
  return SpreadsheetApp.getActiveSheet()
    .getRange(row + offset, col)
    .setValues([ [text] ]);
}


function validate(row, col, plateId) {
  if (col != 1) {
    throw 'Plate IDs must go into the first column.';
  }

  if (!isUuid(plateId)) {
    throw 'Please select a cell with a valid plate ID.';
  }

  if (uuidInSheet(plateId)) {
    throw 'This plate ID "' + plateId + '" has already been entered.';
  }

  if (!cellsAreEmpty(row, col)) {
    throw 'This will overwrite data already in the sheet.';
  }
}


function isUuid(uuid) {
  return uuid.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
}


function uuidInSheet(uuid) {
  count = 0;
  const values = SpreadsheetApp.getActiveSheet()
    .getDataRange()
    .getValues();
  for (var r = 0; r < values.length; r++) {
    for (var c = 0; c < values[r].length; c++) {
      count += values[r][c] == uuid ? 1 : 0;
    }
  }
  return count > 1;
}


function cellsAreEmpty(row, col) {
  const sheet = SpreadsheetApp.getActiveSheet();
  const grid = sheet.getRange(row + TABLE_OFFSET, col + 1, ROW_COUNT, COL_COUNT);
  const bgColors = grid.getBackgrounds();

  for (var r = 0; r < bgColors.length; r++) {
    for (var c = 0; c < bgColors[r].length; c++) {
      if (bgColors[r][c] == BACKGROUND) {
        return false;
      }
    }
  }

  return sheet.getRange(row + DATE_OFFSET, col, TABLE_OFFSET + ROW_COUNT, 1).isBlank() &&
    grid.isBlank();
}


function addRowLabels(row, col) {
  const values = [];
  for (r = 1; r <= ROW_COUNT; ++r) {
    values.push(['Plate row ' + r]);
  }
  SpreadsheetApp.getActiveSheet()
    .getRange(row + TABLE_OFFSET, 1, ROW_COUNT)
    .setValues(values)
    .setBorder(true, true, true, true, true, true);
}


function addBody(row, col) {
  SpreadsheetApp.getActiveSheet()
    .getRange(row + TABLE_OFFSET, col + 1, ROW_COUNT, COL_COUNT)
    .setBackground(BACKGROUND)
    .setBorder(true, true, true, true, true, true);
}

function moveAcross(row, col) {
  if (col >= 2 and col <= COL_COUNT + 1) {
    SpreadsheetApp.getActiveSheet()
      .getRange(row, col + 1)
      .activate();
  }
}

function onEdit(evt) {
  var row = evt.range.getRow();
  var col = evt.range.getColumn();
  moveAcross();
}
