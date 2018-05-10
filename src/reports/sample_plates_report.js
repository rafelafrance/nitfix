////////////////////////////////////////////////////////////////////////////////////
// Open close report sections

document.querySelectorAll('h2').forEach(function(header) {
  header.addEventListener('click', function(event) {
    const button = event.target.matches('button') ? event.target : event.target.querySelector('button');
    button.classList.toggle('closed');
    const selector = '.' + button.dataset.selector;
    document.querySelector(selector).classList.toggle('closed');
  });
});

////////////////////////////////////////////////////////////////////////////////////
// Toggle a group of Family Coverage rows open/closed. That is we will show/hide
// all genus records for a family. Closing them leaves only the first record
// (the family one) visible.

document.querySelector('section.families tbody')
  .addEventListener('click', function(event) {
    if (! event.target.matches('button')) { return; }
    event.target.classList.toggle('closed');
    const family = event.target.dataset.family;
    const selector = '.families tr[data-family="' + family + '"]';
    const trs = document.querySelectorAll(selector);
    trs.forEach(function(r) { r.classList.toggle('closed'); });
});


////////////////////////////////////////////////////////////////////////////////////
// Data passed in from python for the Sample Plates report section

const allPlates = {{ plates | safe }};
const allWells = {{ wells | safe }};

////////////////////////////////////////////////////////////////////////////////////
// Filter logic for the Sample Plates report section

var filteredPlates = allPlates.filter(function(plate) { return true; });

function debounce(func, wait, immediate) {
  // Taken from underscore.js
  var timeout;
  return function() {
    var context = this, args = arguments;
    var later = function() {
      timeout = null;
      if (!immediate) func.apply(context, args);
    };
    var callNow = immediate && !timeout;
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
    if (callNow) { func.apply(context, args) };
  };
};


function filterWells(plate) {
  const scientific_name = (document.querySelector('#search-sci-name').value || '').toLowerCase();
  const family = (document.querySelector('#search-family').value || '').toLowerCase();
  const sample_id = (document.querySelector('#search-sample-id').value || '').toLowerCase();
  const sent2Rapid = (document.querySelector('#search-sent-to-rapid').checked);
  const seqReturned = (document.querySelector('#search-seq-returned').checked);
  return allWells[plate.plate_id]
    .filter(function (well) { return sample_id ? well.sample_id.toLowerCase().indexOf(sample_id) > -1 : true; })
    .filter(function (well) { return scientific_name ? well.scientific_name.toLowerCase().indexOf(scientific_name) > -1 : true; })
    .filter(function (well) { return family ? well.family.toLowerCase().indexOf(family) > -1 : true; })
    .filter(function (well) { return sent2Rapid ? well.rapid_concentration : true; })
    .filter(function (well) { return seqReturned ? well.seqReturned : true; });
}

function filterChange(target) {
  filteredPlates = allPlates.filter(function(plate) {
    const filtered = filterWells(plate);
    return filtered.length;
  });
  resetPager();
}


document.querySelectorAll('section.samples .search-control input[type=text]')
  .forEach(function(input) {
    input.value = '';
    filterChange(input);
    input.addEventListener('keyup', debounce(function(event) {
      filterChange(event.target);
    }, 250));
});

document.querySelectorAll('section.samples .search-control input[type=checkbox]')
  .forEach(function(input) {
    filterChange(input);
    input.addEventListener('change', function(event) {
      filterChange(event.target);
    });
});


document.querySelectorAll('section.samples .search-control button')
  .forEach(function(btn) {
    btn.addEventListener('click', debounce(function(event) {
      const input = btn.previousElementSibling;
      input.value = '';
      filterChange(input);
    }));
});


////////////////////////////////////////////////////////////////////////////////////
// Pager logic for the Sample Plates report section

var maxPage;

function changePage() {
  const pager = document.querySelector('.pager');
  var page = +pager.value;
  page = page < 1 ? 1 : page;
  page = page > maxPage ? maxPage : page;
  pager.value = page;
  const rows = buildTableRows(page);
  buildTable(rows);
}

document.querySelector('.first-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = 1;
  changePage();
});

document.querySelector('.previous-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = +pager.value - 1;
  changePage();
});

document.querySelector('.next-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = +pager.value + 1;
  changePage();
});

document.querySelector('.last-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = maxPage;
  changePage();
});

document.querySelector('.pager').addEventListener('change', changePage);

function resetPager() {
  maxPage = filteredPlates.length;
  document.querySelector('section.samples .max-page').innerHTML = 'of ' + maxPage;
  document.querySelector('section.samples .pager').value = '1';
  changePage();
}

resetPager();

////////////////////////////////////////////////////////////////////////////////////
// Logic for build the Sample Plates table

function buildTable(rows) {
  const tbody = document.querySelector('section.samples tbody');

  // Remove old rows
  while (tbody && tbody.firstChild) {
    tbody.removeChild(tbody.firstChild);
  }

  // Add new rows
  rows.forEach(function(row) {

    // Build the table's rows
    const tr = document.createElement('tr');
    if (row.cls) { tr.classList.add(row.cls); }

    // Build the row's cells
    row.td.forEach(function(cell) {
      const td = document.createElement('td');
      if (cell.cls) { td.classList.add(cell.cls); }
      td.innerHTML = cell.content;
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

}

function buildPlateHeader() {
  return {
    cls: 'header',
    td: [
      { content: 'Plate ID' },
      { content: 'Entry Date' },
      { content: 'Local ID' },
      { content: 'Protocol' },
      { content: 'Notes' },
      { content: '' },
      { content: '' },
      { content: '' },
      { content: '' },
    ],
  };
}

function buildPlateData(plate) {
  return {
    td: [
      { content: plate.plate_id, cls: 'l' },
      { content: plate.entry_date },
      { content: plate.local_id, cls: 'l' },
      { content: plate.protocol, cls: 'l' },
      { content: plate.notes,    cls: 'l' },
    ],
  };
}

function buildWellHeader() {
  return {
    cls: 'sub-header',
    td: [
      { content: '', cls: 'empty'},
      { content: 'Well' },
      { content: 'Well Number' },
      { content: 'Family' },
      { content: 'Scientific Name' },
      { content: 'Mean Yield (ng/µL)' },
      { content: 'Sent to Rapid' },
      { content: 'Sequence Returned' },
      { content: 'Sample ID' },
    ],
  };
}

function buildWellData(well) {
  const sent2Rapid = well.rapid_concentration ? 'Yes' : '';
  const seqReturned = '';
  var mean = parseFloat(well.ng_microliter_mean).toFixed(3);
  mean = mean == 'NaN' ? '' : mean;
  return {
    cls: 'well',
    td: [
      { content: '', cls: 'empty' },
      { content: well.well },
      { content: well.picogreen_id },
      { content: well.family,          cls: 'l' },
      { content: well.scientific_name, cls: 'l' },
      { content: mean,                 cls: 'r' },
      { content: sent2Rapid,                    },
      { content: seqReturned,                   },
      { content: well.sample_id,       cls: 'l' },
    ],
  };
}

function buildTableRows(page) {
  const rows = [];
  if (page) {
    const plate = filteredPlates[page - 1];
    rows.push(buildPlateHeader());
    rows.push(buildPlateData(plate));
    rows.push(buildWellHeader());
    filterWells(plate).forEach(function(well) {
      rows.push(buildWellData(well));
    });
  }
  return rows;
}
