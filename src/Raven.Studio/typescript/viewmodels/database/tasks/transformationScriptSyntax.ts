import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class transformationScriptSyntax extends dialogViewModelBase {

    etlType = ko.observable<Raven.Client.Documents.Operations.ETL.EtlType>();
    htmlElement: HTMLElement;

    constructor(etlType: Raven.Client.Documents.Operations.ETL.EtlType) {
        super();
        this.etlType(etlType);
    }
    
    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.htmlElement = document.getElementById("transformationScriptSyntaxDialog");
    }

    copySample(sampleTitle?: string) {
        let sampleText;
        
        if (this.etlType() === "Raven") {
            sampleText = transformationScriptSyntax.ravenEtlSamples.find(x => x.title === sampleTitle).text;
        } else {
            sampleText = transformationScriptSyntax.sqlEtlSampleText;
        }
        
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.htmlElement);
    }

    static readonly ravenEtlSamples: Array<sampleCode> = [
        {
            title: "Simple Transformation Script",
            text:
`loadToNewEmployees ({
    Name: this.LastName + " some new data..",
    Title: this.Title + " some new data.."
});`,
            html:
`<span class="token keyword">loadTo<span class="token string">NewEmployees</span></span><span class="token punctuation"> ({</span>
    Name: this.LastName <span class="token operator">+</span> " some new data..",
    Title: this.Title <span class="token operator">+</span> " some new data.."
<span class="token punctuation">})</span>;`
        },
        {
            title: "Transformation Script with custom logic for counters",
            text:
`loadToUsers ({
    Name: this.Name + " some new data.."
});

function loadCountersOfUsersBehavior(documentId, counterName) {
    return true;
}`,
            html:
`<span class="token keyword">loadTo<span class="token string">Users</span></span><span class="token punctuation"> ({</span>
    Name: this.Name <span class="token operator">+</span> " some new data.."
<span class="token punctuation">})</span>;

function <span class="token keyword">loadCountersOf<span class="token string">Users</span>Behavior</span>(documentId, counterName) {
    return true;
}`
        },
        {
            title: "Transformation Script with custom logic for documents deletion",
            text:
`loadToUsers ({
    Name: this.Name + " some new data.."
});

function deleteDocumentsOfUsersBehavior(documentId) {
    return false; // don't propagate document deletions
}`,
            html:
`<span class="token keyword">loadTo<span class="token string">Users</span></span><span class="token punctuation"> ({</span>
    Name: this.Name <span class="token operator">+</span> " some new data.."
<span class="token punctuation">})</span>;

function <span class="token keyword">deleteDocumentsOf<span class="token string">Users</span>Behavior</span>(documentId) {
    return false; <span class="token comment">// don't propagate document deletions</span>
}`
        }
    ];
    
    static readonly sqlEtlSampleText =
 `var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += line.PricePerUnit;
    
    // Load to SQL table 'OrderLines'
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}
orderData.TotalCost = Math.round(orderData.TotalCost * 100) / 100;

// Load to SQL table 'Orders'
loadToOrders(orderData);`;

    sqlEtlSampleHtml = ko.pureComputed(() => {
        return Prism.highlight(transformationScriptSyntax.sqlEtlSampleText, (Prism.languages as any).javascript);
    });
}

export = transformationScriptSyntax;
