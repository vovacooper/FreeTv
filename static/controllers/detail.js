angular.module('MyApp')
    .controller('DetailCtrl', function ($scope, $rootScope, $routeParams, Show, Subscription, $modal) {
        Show.get({_id: $routeParams.id}, function (show) {
            $scope.show = show;

            $scope.nextEpisode = show.episodes.filter(function (episode) {
                return new Date(episode.firstAired) > new Date();
            })[0];
        });

    });

angular.module('MyApp')
    .controller('ShowModalCtrl', function ($scope, $rootScope, $routeParams, Show, link) {
        $scope.link = angular.copy(link);
    });