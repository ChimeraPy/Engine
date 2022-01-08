import QtQuick 2.0

// Custom Imports Items
import Dashboard 1.0
import "../diagrams"

Item {

    Rectangle {
        id: rectangle
        color: "#2d2d2d"
        anchors.fill: parent

        ListView {
            id: listView
            anchors.margins: 15
            anchors.fill: parent
            spacing: 50
            orientation: ListView.Vertical
            model: DashboardModel {}
            delegate: BaseDiagramDelegate {}
        }
    }

    Connections{
        target: backend
    }
}

/*##^##
Designer {
    D{i:2;anchors_height:160;anchors_width:813;anchors_x:154;anchors_y:132}D{i:1;anchors_height:400;anchors_width:1000;anchors_x:0;anchors_y:0}
}
##^##*/
