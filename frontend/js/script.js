const ctx = document.getElementById('sentiment-chart');

  new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: [
      'Negative',
      'Neutral',
      'Positive'
      ],

      datasets: [{
      label: 'My First Dataset',
      data: [300, 50, 100],
      backgroundColor: [
      'rgb(255, 99, 132)',
      'rgb(54, 162, 235)',
      'rgb(255, 205, 86)'
      ],
      hoverOffset: 4
      }]    
    }});

  const line = document.getElementById('prediction-chart');

  new Chart(line, {
    type: 'line',
    data: {
      labels: [
      'ja','fe','ma', 'ap', 'me', 'jn', 'jl'
      ],

      datasets: [{
        label: 'Prediction Close Price',
        data: [65, 59, 80, 81, 56, 55, 40],
        fill: false,
        borderColor: 'rgb(75, 192, 192)',
        tension: 0.1
        }] 
    }});



$(document).ready(function () {
  $(".data-table").each(function (_, table) {
    $(table).DataTable();
  });
});


