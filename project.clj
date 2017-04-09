(defproject tranchis/photon-mongo "0.10.3"
  :description "MongoDB backend plugin for photon"
  :url "https://github.com/microserviceux/photon-mongo"
  :license {:name "GNU Affero General Public License Version 3"
            :url "https://www.gnu.org/licenses/agpl-3.0.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [tranchis/photon-db "0.10.2"]
                 [congomongo "0.5.0"]
                 [midje "1.8.3"]
                 [org.clojure/tools.logging "0.3.1"]])
