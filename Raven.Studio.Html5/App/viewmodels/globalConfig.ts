import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import status = require("viewmodels/status");
import adminSettings = require('viewmodels/adminSettings');

class globalConfig extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

	constructor() {
        super();

        this.router = adminSettings.adminSettingsRouter.createChildRouter()
            .map([
                { route: 'admin/settings/globalConfig', moduleId: 'viewmodels/globalConfigPeriodicExport', title: 'Periodic export', tooltip: "", nav: true, hash: appUrl.forGlobalConfigPeriodicExport() },
                { route: 'admin/settings/globalConfigReplication', moduleId: 'viewmodels/globalConfigReplications', title: 'Replication', tooltip: 'Global replication settings', nav: true, hash: appUrl.forGlobalConfigReplication() },
                { route: 'admin/settings/globalConfigQuotas', moduleId: 'viewmodels/globalConfigQuotas', title: 'Quotas', tooltip: 'Global quotas settings', nav: true, hash: appUrl.forGlobalConfigQuotas() }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = globalConfig;    