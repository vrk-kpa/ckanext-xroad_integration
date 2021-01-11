'use strict';

ckan.module('xroad_stats', function ($) {
  return {
    initialize: function() {
      console.log('xroad_stats.initialize')

      let data = xroadServicesGraphData.concat();
      let distinctServiceCounts = data.map(d => d.distinctServiceCount)
      let dataMinValue = Math.min.apply(null, distinctServiceCounts)
      let dataMaxValue = Math.max.apply(null, distinctServiceCounts)

      let ctx = document
        .getElementById('serviceCountCanvas')
        .getContext('2d');

      let chart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: data.map(d => d.date),
          datasets: [
            {
              data: distinctServiceCounts,
              label: 'Distinct services',
              borderColor: 'rgb(26, 163, 255)',
              fill: true,
              backgroundColor: 'rgba(26, 163, 255, 0.2)',
              cubicInterpolationMode: 'monotone',
            },
            {
              data: data.map(d => d.restServiceCount),
              label: 'REST services',
              borderColor: 'rgb(255, 99, 132)',
              fill: true,
              backgroundColor: 'rgba(255, 99, 132, 0.2)',
              cubicInterpolationMode: 'monotone',
            },
            {
              data: data.map(d => d.soapServiceCount),
              label: 'SOAP services',
              borderColor: 'rgb(255, 206, 86)',
              fill: true,
              backgroundColor: 'rgba(255, 206, 86, 0.2)',
              cubicInterpolationMode: 'monotone',
            },
          ],
        },
        options: {
          title: {
            display: true,
            text: 'Services timeline',
            fontSize: 16,
          },
          scales: {
            yAxes: [
              {
                ticks: {
                  suggestedMax: dataMaxValue + dataMaxValue * 0.2,
                  suggestedMin: dataMinValue - dataMinValue * 0.2,
                },
                gridLines: {
                  display: false,
                },
              },
            ],
          },
          legend: {
            labels: {
              fontSize: 14,
            },
          },
          plugins: {
            datalabels: {
              align: 100,
              anchor: 'start',
              font: {
                weight: 'bold',
                size: 14,
              },
            },
          },
          animation: {
            duration: 0, // general animation time
          },
        },
      })
    }
  }
})

