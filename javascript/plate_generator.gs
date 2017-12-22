var ROW_COUNT = 8;
var COL_COUNT = 12;
var LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
var BACKGROUND = '#fdffa8';


function setup() {
  const sheet = SpreadsheetApp.getActiveSheet();

  const values = [[]];
  values[0].push('Plate ID');
  for (c = 0; c < COL_COUNT; c++) {
    values[0].push('Plate column ' + LETTERS[c]);
  }
  sheet.getRange(1, 1, 1, COL_COUNT + 1 ).setValues(values);

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

  addRowLabels(row, col);
  addBody(row, col);
}


function validate(row, col, plateId) {
  if (col != 1) {
    throw 'Plate IDs must go into the first column.';
  }

  if (!validUuid(plateId)) {
    throw 'Please select a cell with a valid plate ID.';
  }

  if (plateIdInSheet(plateId)) {
    throw 'This plate ID "' + plateId + '" has already been entered.';
  }

  if (!cellsAreEmpty(row, col)) {
    throw 'This will overwrite data already in the sheet.';
  }
}


function validUuid(uuid) {
  return uuid.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
}


function plateIdInSheet(plateId) {
  const values = SpreadsheetApp.getActiveSheet()
    .getDataRange()
    .getValues();
  count = 0;
  for (var r = 0; r < values.length; r++) {
    for (var c = 0; c < values[r].length; c++) {
      count += values[r][c] == plateId ? 1 : 0;
    }
  }
  return count > 1;
}


function cellsAreEmpty(row, col) {
  const sheet = SpreadsheetApp.getActiveSheet();
  const grid = sheet.getRange(row + 1, col + 1, ROW_COUNT, COL_COUNT);
  const bgColors = grid.getBackgrounds();

  for (var r = 0; r < bgColors.length; r++) {
    for (var c = 0; c < bgColors[r].length; c++) {
      if (bgColors[r][c] == BACKGROUND) { return false; }
    }
  }

  return sheet.getRange(row + 1, col, ROW_COUNT, 1).isBlank()
      && grid.isBlank();
}


function addRowLabels(row, col) {
  const values = [];
  for (r = 1; r <= ROW_COUNT; ++r) {
    values.push(['Plate row ' + r]);
  }
  SpreadsheetApp.getActiveSheet()
    .getRange(row + 1, 1, ROW_COUNT, 1)
    .setValues(values);
}


function addBody(row, col) {
  SpreadsheetApp.getActiveSheet()
    .getRange(row + 1, col + 1, ROW_COUNT, COL_COUNT)
    .setBackground(BACKGROUND)
    .setBorder(true, true, true, true, true, true);
}
