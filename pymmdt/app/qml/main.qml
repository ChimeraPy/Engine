import QtQuick 2.12
import QtQuick.Window 2.12
import QtQuick.Controls 2.12

Window {
    id: root
    width: 1000
    height: 580
    visible: true
    title: qsTr("PyMMDT Dashboard")

    Rectangle {
        id: appContainer
        color: "#2d2d2d"
        anchors.right: parent.right
        anchors.rightMargin: 0
        anchors.left: parent.left
        anchors.leftMargin: 0
        anchors.bottom: parent.bottom
        anchors.bottomMargin: 0
        anchors.top: parent.top
        anchors.topMargin: 0

        Rectangle {
            id: topBar
            height: 52
            color: "#0f0f0f"
            anchors.right: parent.right
            anchors.rightMargin: 0
            anchors.left: parent.left
            anchors.leftMargin: 0
            anchors.top: parent.top
            anchors.topMargin: 0
        }

        Rectangle {
            id: timeTrack
            height: 200
            color: "#0f0f0f"
            anchors.bottom: parent.bottom
            anchors.bottomMargin: 0
            anchors.right: parent.right
            anchors.rightMargin: 0
            anchors.left: parent.left
            anchors.leftMargin: 0

            Button {
                id: closingTimetrack
                height: 8
                anchors.right: parent.right
                anchors.rightMargin: 0
                anchors.left: parent.left
                anchors.leftMargin: 0
                anchors.top: parent.top
                anchors.topMargin: 0
                onClicked: {
                    if (timeTrack.height > closingTimetrack.height)
                        timeTrack.height = closingTimetrack.height
                    else
                        timeTrack.height = 200
                }
            }
        }

        Rectangle {
            id: content
            color: "#2d2d2d"
            anchors.top: topBar.bottom
            anchors.topMargin: 0
            anchors.bottom: timeTrack.top
            anchors.bottomMargin: 0
            anchors.right: parent.right
            anchors.rightMargin: 0
            anchors.left: parent.left
            anchors.leftMargin: 0

            Loader {
                id: pageLoader
                anchors.fill: parent
                source: Qt.resolvedUrl("pages/homePage.qml")
            }
        }

        Button {
            id: appButton
            width: 100
            text: qsTr("PyMMDT")
            transformOrigin: Item.Center
            font.pointSize: 13
            anchors.bottom: content.top
            anchors.bottomMargin: 0
            anchors.left: parent.left
            anchors.leftMargin: 0
            anchors.top: parent.top
            anchors.topMargin: 0
        }
    }
}



/*##^##
Designer {
    D{i:2;anchors_width:1000;anchors_x:0;anchors_y:0}D{i:4;anchors_width:1000;anchors_x:0;anchors_y:0}
D{i:3;anchors_width:1000;anchors_x:0}D{i:5;anchors_height:327;anchors_width:1000;anchors_x:0;anchors_y:50}
D{i:7;anchors_height:51;anchors_x:0;anchors_y:0}D{i:1;anchors_height:580;anchors_width:1000;anchors_x:0;anchors_y:0}
}
##^##*/
