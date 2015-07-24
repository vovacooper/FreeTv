angular.module('MyApp')
  .controller('DetailCtrl', function($scope, $rootScope, $routeParams, Show, Subscription) {
      Show.get({ _id: $routeParams.id }, function(show) {
        $scope.show = show;

        $scope.nextEpisode = show.episodes.filter(function(episode) {
          return new Date(episode.firstAired) > new Date();
        })[0];
      });
    });