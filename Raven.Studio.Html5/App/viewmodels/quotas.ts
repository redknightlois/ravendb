﻿import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/saveDatabaseSettingsCommand");
import getConfigurationSettingsCommand = require('commands/getConfigurationSettingsCommand');
import document = require("models/document");
import database = require("models/database");
import appUrl = require("common/appUrl");
import configurationSetting = require('models/configurationSetting');
import configurationSettings = require('models/configurationSettings');

class quotas extends viewModelBase {
    settingsDocument = ko.observable<document>();

    maximumSize: configurationSetting;
    warningLimitThreshold: configurationSetting;
    maxNumberOfDocs: configurationSetting;
    warningThresholdForDocs: configurationSetting;
 
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            this.fetchQuotas(db)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);

        this.initializeDirtyFlag();

        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty() === true);
    }

    private fetchQuotas(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        var dbSettingsTask = new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((doc: document) => this.settingsDocument(doc));

        var configTask = new getConfigurationSettingsCommand(db,
            ["Raven/Quotas/Size/HardLimitInKB", "Raven/Quotas/Size/SoftMarginInKB", "Raven/Quotas/Documents/HardLimit", "Raven/Quotas/Documents/SoftLimit"])
            .execute()
            .done((result: configurationSettings) => {
                this.maximumSize = result.results["Raven/Quotas/Size/HardLimitInKB"];
                this.warningLimitThreshold = result.results["Raven/Quotas/Size/SoftMarginInKB"];
                this.maxNumberOfDocs = result.results["Raven/Quotas/Documents/HardLimit"];
                this.warningThresholdForDocs = result.results["Raven/Quotas/Documents/SoftLimit"];

                var divideBy1024 = (x: KnockoutObservable<any>) => {
                    if (x()) {
                        x(x() / 1024);
                    }
                }

                divideBy1024(this.maximumSize.effectiveValue);
                divideBy1024(this.maximumSize.globalValue);
                divideBy1024(this.warningLimitThreshold.effectiveValue);
                divideBy1024(this.warningLimitThreshold.globalValue);
            });

        return $.when(dbSettingsTask, configTask);
    }

    initializeDirtyFlag() {
        this.dirtyFlag = new ko.DirtyFlag([
            this.maximumSize.effectiveValue, this.maximumSize.localExists,
            this.warningLimitThreshold.effectiveValue, this.warningLimitThreshold.localExists,
            this.maxNumberOfDocs.effectiveValue, this.maxNumberOfDocs.localExists,
            this.warningThresholdForDocs.effectiveValue, this.warningThresholdForDocs.localExists
        ]);
    }

    saveChanges() {
        var db = this.activeDatabase();
        if (db) {
            var settingsDocument = this.settingsDocument();
            settingsDocument['@metadata'] = this.settingsDocument().__metadata;
            settingsDocument['@metadata']['@etag'] = this.settingsDocument().__metadata['@etag'];
            var doc = new document(settingsDocument.toDto(true));
            if (this.maximumSize.canEdit()) {
                doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"] = <any>this.maximumSize.effectiveValue() * 1024;
            } else {
                delete doc["Settings"]["Raven/Quotas/Size/HardLimitInKB"];
            }
            if (this.warningLimitThreshold.canEdit()) {
                doc["Settings"]["Raven/Quotas/Size/SoftMarginInKB"] = <any>this.warningLimitThreshold.effectiveValue() * 1024;
            } else {
                delete doc["Settings"]["Raven/Quotas/Size/SoftMarginInKB"];
            }
            if (this.maxNumberOfDocs.canEdit()) {
                doc["Settings"]["Raven/Quotas/Documents/HardLimit"] = this.maxNumberOfDocs.effectiveValue();
            } else {
                delete doc["Settings"]["Raven/Quotas/Documents/HardLimit"];
            }
            if (this.warningThresholdForDocs.canEdit()) {
                doc["Settings"]["Raven/Quotas/Documents/SoftLimit"] = this.warningThresholdForDocs.effectiveValue();
            } else {
                delete doc["Settings"]["Raven/Quotas/Documents/SoftLimit"];
            }
            
            var saveTask = new saveDatabaseSettingsCommand(db, doc).execute();
            saveTask.done((saveResult: databaseDocumentSaveDto) => {
                this.settingsDocument().__metadata['@etag'] = saveResult.ETag;
                this.dirtyFlag().reset(); //Resync Changes
            });
        }
    }

    override(config: configurationSetting) {
        if (!config.localExists()) {
            config.copyFromGlobal();
        }
        return true;
    }
}

export = quotas;
